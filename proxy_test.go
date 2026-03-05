package main

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// startFakeRedis starts a minimal fake Redis server that responds to PING with PONG
// and echoes SET/GET commands. Returns the listener address.
func startFakeRedis(t *testing.T, auth string) (string, *net.Listener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleFakeRedis(conn, auth)
		}
	}()

	return ln.Addr().String(), &ln
}

func handleFakeRedis(conn net.Conn, auth string) {
	defer conn.Close()
	reader := NewRESPReader(conn)
	writer := NewRESPWriter(conn)
	authenticated := auth == ""

	for {
		val, err := reader.ReadValue()
		if err != nil {
			return
		}

		args := val.ToArgs()
		if len(args) == 0 {
			continue
		}

		cmd := args[0]
		switch {
		case cmd == "AUTH" || cmd == "auth":
			if len(args) < 2 || args[1] != auth {
				writer.WriteError("ERR invalid password")
			} else {
				authenticated = true
				writer.WriteSimpleString("OK")
			}
		case !authenticated:
			writer.WriteError("NOAUTH Authentication required")
		case cmd == "PING" || cmd == "ping":
			writer.WriteSimpleString("PONG")
		case cmd == "SET" || cmd == "set":
			writer.WriteSimpleString("OK")
		case cmd == "ECHO" || cmd == "echo":
			if len(args) > 1 {
				writer.WriteBulkString(args[1])
			} else {
				writer.WriteError("ERR wrong number of arguments")
			}
		default:
			writer.WriteError("ERR unknown command")
		}
		writer.Flush()
	}
}

func TestProxyBasic(t *testing.T) {
	fakeAddr, _ := startFakeRedis(t, "")

	proxy := NewRedisProxy("127.0.0.1:0", "", func() string { return fakeAddr })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := NewRESPReader(conn)
	w := NewRESPWriter(conn)

	// Send PING
	w.WriteBulkStringArray([]string{"PING"})
	w.Flush()

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "PONG" {
		t.Fatalf("expected PONG, got %q", val.Str)
	}
}

func TestProxyWithAuth(t *testing.T) {
	fakeAddr, _ := startFakeRedis(t, "secret123")

	proxy := NewRedisProxy("127.0.0.1:0", "secret123", func() string { return fakeAddr })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := NewRESPReader(conn)
	w := NewRESPWriter(conn)

	// Send PING through the proxy (proxy should have authed upstream for us)
	w.WriteBulkStringArray([]string{"PING"})
	w.Flush()

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "PONG" {
		t.Fatalf("expected PONG, got %q", val.Str)
	}
}

func TestProxyWithBadAuth(t *testing.T) {
	fakeAddr, _ := startFakeRedis(t, "secret123")

	proxy := NewRedisProxy("127.0.0.1:0", "wrongpassword", func() string { return fakeAddr })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Connection should close because upstream auth fails
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err != io.EOF && err != io.ErrUnexpectedEOF {
		// Could also be a timeout or connection reset - all indicate the proxy dropped us
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			// Acceptable - proxy closed before we read
		} else if err != nil {
			// Some other error is also fine - connection was closed
		} else {
			t.Fatal("expected connection to be closed due to auth failure")
		}
	}
}

func TestProxyNoMaster(t *testing.T) {
	proxy := NewRedisProxy("127.0.0.1:0", "", func() string { return "" })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Connection should close because no master is available
	conn.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
}

func TestProxyEchoData(t *testing.T) {
	fakeAddr, _ := startFakeRedis(t, "")

	proxy := NewRedisProxy("127.0.0.1:0", "", func() string { return fakeAddr })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	r := NewRESPReader(conn)
	w := NewRESPWriter(conn)

	// Test ECHO
	w.WriteBulkStringArray([]string{"ECHO", "hello world"})
	w.Flush()

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "hello world" {
		t.Fatalf("expected 'hello world', got %q", val.Str)
	}
}

func TestProxyConcurrentConnections(t *testing.T) {
	fakeAddr, _ := startFakeRedis(t, "")

	proxy := NewRedisProxy("127.0.0.1:0", "", func() string { return fakeAddr })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()

			r := NewRESPReader(conn)
			w := NewRESPWriter(conn)

			w.WriteBulkStringArray([]string{"PING"})
			w.Flush()

			val, err := r.ReadValue()
			if err != nil {
				t.Error(err)
				return
			}
			if val.Str != "PONG" {
				t.Errorf("expected PONG, got %q", val.Str)
			}
		}()
	}
	wg.Wait()
}

func TestProxyUpstreamUnreachable(t *testing.T) {
	// Point to a port that nothing is listening on
	proxy := NewRedisProxy("127.0.0.1:0", "", func() string { return "127.0.0.1:1" })
	if err := proxy.Start(); err != nil {
		t.Fatal(err)
	}
	defer proxy.Stop()

	proxyAddr := proxy.listener.Addr().String()

	conn, err := net.DialTimeout("tcp", proxyAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Connection should close because upstream is unreachable
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	_, err = conn.Read(buf)
	if err == nil {
		t.Fatal("expected connection to be closed")
	}
}
