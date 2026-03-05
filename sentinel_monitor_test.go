package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeSentinel simulates an upstream Sentinel for testing.
type fakeSentinel struct {
	listener   net.Listener
	masterIP   string
	masterPort string
	masterName string
	auth       string

	mu          sync.Mutex
	subscribers []net.Conn
}

func startFakeSentinel(t *testing.T, masterName, masterIP, masterPort, auth string) *fakeSentinel {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeSentinel{
		listener:   ln,
		masterIP:   masterIP,
		masterPort: masterPort,
		masterName: masterName,
		auth:       auth,
	}
	t.Cleanup(func() {
		ln.Close()
		fs.mu.Lock()
		for _, c := range fs.subscribers {
			c.Close()
		}
		fs.mu.Unlock()
	})
	go fs.acceptLoop()
	return fs
}

func (fs *fakeSentinel) Addr() string {
	return fs.listener.Addr().String()
}

func (fs *fakeSentinel) SetMaster(ip, port string) {
	fs.mu.Lock()
	fs.masterIP = ip
	fs.masterPort = port
	fs.mu.Unlock()
}

func (fs *fakeSentinel) SendSwitchMaster(oldIP, oldPort, newIP, newPort string) {
	payload := fmt.Sprintf("%s %s %s %s %s", fs.masterName, oldIP, oldPort, newIP, newPort)

	fs.mu.Lock()
	subs := make([]net.Conn, len(fs.subscribers))
	copy(subs, fs.subscribers)
	fs.mu.Unlock()

	for _, conn := range subs {
		w := NewRESPWriter(conn)
		w.WriteArrayHeader(3)
		w.WriteBulkString("message")
		w.WriteBulkString("+switch-master")
		w.WriteBulkString(payload)
		w.Flush()
	}
}

func (fs *fakeSentinel) acceptLoop() {
	for {
		conn, err := fs.listener.Accept()
		if err != nil {
			return
		}
		go fs.handleConn(conn)
	}
}

func (fs *fakeSentinel) handleConn(conn net.Conn) {
	defer conn.Close()
	reader := NewRESPReader(conn)
	writer := NewRESPWriter(conn)

	for {
		val, err := reader.ReadValue()
		if err != nil {
			return
		}

		args := val.ToArgs()
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "AUTH":
			if fs.auth == "" || (len(args) > 1 && args[1] == fs.auth) {
				writer.WriteSimpleString("OK")
			} else {
				writer.WriteError("ERR invalid password")
			}
			writer.Flush()

		case "SENTINEL":
			if len(args) >= 3 && strings.ToUpper(args[1]) == "GET-MASTER-ADDR-BY-NAME" {
				if args[2] == fs.masterName {
					fs.mu.Lock()
					ip := fs.masterIP
					port := fs.masterPort
					fs.mu.Unlock()
					writer.WriteBulkStringArray([]string{ip, port})
				} else {
					writer.WriteNullArray()
				}
				writer.Flush()
			}

		case "SUBSCRIBE":
			// Send subscription ack
			for i, ch := range args[1:] {
				writer.WriteArrayHeader(3)
				writer.WriteBulkString("subscribe")
				writer.WriteBulkString(ch)
				writer.WriteInteger(int64(i + 1))
			}
			writer.Flush()

			// Register subscriber
			fs.mu.Lock()
			fs.subscribers = append(fs.subscribers, conn)
			fs.mu.Unlock()

			// Block - keep connection open
			buf := make([]byte, 1)
			conn.Read(buf)
			return
		}
	}
}

func TestSentinelMonitorPoll(t *testing.T) {
	fs := startFakeSentinel(t, "mymaster", "10.0.0.1", "6379", "")

	var mu sync.Mutex
	var lastMaster string
	onChange := func(addr string) {
		mu.Lock()
		lastMaster = addr
		mu.Unlock()
	}

	sm := NewSentinelMonitor(fs.Addr(), "mymaster", "", onChange)
	sm.Start()
	defer sm.Stop()

	// Wait for first poll
	time.Sleep(200 * time.Millisecond)

	current := sm.CurrentMaster()
	if current != "10.0.0.1:6379" {
		t.Fatalf("expected 10.0.0.1:6379, got %q", current)
	}

	mu.Lock()
	got := lastMaster
	mu.Unlock()
	if got != "10.0.0.1:6379" {
		t.Fatalf("onChange expected 10.0.0.1:6379, got %q", got)
	}
}

func TestSentinelMonitorMasterChange(t *testing.T) {
	fs := startFakeSentinel(t, "mymaster", "10.0.0.1", "6379", "")

	changes := make(chan string, 10)
	sm := NewSentinelMonitor(fs.Addr(), "mymaster", "", func(addr string) {
		changes <- addr
	})
	sm.Start()
	defer sm.Stop()

	// Wait for initial discovery
	select {
	case addr := <-changes:
		if addr != "10.0.0.1:6379" {
			t.Fatalf("initial master: expected 10.0.0.1:6379, got %q", addr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for initial master")
	}

	// Change master
	fs.SetMaster("10.0.0.2", "6380")

	// Wait for poll to detect change
	select {
	case addr := <-changes:
		if addr != "10.0.0.2:6380" {
			t.Fatalf("new master: expected 10.0.0.2:6380, got %q", addr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for master change")
	}
}

func TestSentinelMonitorWithAuth(t *testing.T) {
	fs := startFakeSentinel(t, "mymaster", "10.0.0.5", "6379", "sentinelpass")

	changes := make(chan string, 10)
	sm := NewSentinelMonitor(fs.Addr(), "mymaster", "sentinelpass", func(addr string) {
		changes <- addr
	})
	sm.Start()
	defer sm.Stop()

	select {
	case addr := <-changes:
		if addr != "10.0.0.5:6379" {
			t.Fatalf("expected 10.0.0.5:6379, got %q", addr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for master with auth")
	}
}

func TestSentinelMonitorSubscription(t *testing.T) {
	fs := startFakeSentinel(t, "mymaster", "10.0.0.1", "6379", "")

	changes := make(chan string, 10)
	sm := NewSentinelMonitor(fs.Addr(), "mymaster", "", func(addr string) {
		changes <- addr
	})
	sm.Start()
	defer sm.Stop()

	// Wait for initial master
	select {
	case <-changes:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for initial master")
	}

	// Wait for subscriber to connect
	time.Sleep(500 * time.Millisecond)

	// Send switch-master via pub/sub
	fs.SetMaster("10.0.0.3", "6381")
	fs.SendSwitchMaster("10.0.0.1", "6379", "10.0.0.3", "6381")

	// Should get change notification via pub/sub (faster than polling)
	select {
	case addr := <-changes:
		if addr != "10.0.0.3:6381" {
			t.Fatalf("expected 10.0.0.3:6381, got %q", addr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for pub/sub master change")
	}
}

func TestSentinelMonitorSetMasterNoChangeNoDuplicate(t *testing.T) {
	callCount := 0
	var mu sync.Mutex

	sm := &SentinelMonitor{
		onChange: func(addr string) {
			mu.Lock()
			callCount++
			mu.Unlock()
		},
		stopCh: make(chan struct{}),
	}

	sm.setMaster("10.0.0.1:6379")
	sm.setMaster("10.0.0.1:6379") // same value, should not trigger onChange again

	mu.Lock()
	defer mu.Unlock()
	if callCount != 1 {
		t.Fatalf("expected onChange called once, got %d", callCount)
	}
}

func TestSentinelMonitorStopClean(t *testing.T) {
	// Sentinel that nothing listens on - monitor should still stop cleanly
	sm := NewSentinelMonitor("127.0.0.1:1", "mymaster", "", nil)
	sm.Start()

	done := make(chan struct{})
	go func() {
		sm.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Stop() did not return in time")
	}
}
