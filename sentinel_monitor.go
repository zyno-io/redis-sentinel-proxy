package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// SentinelMonitor monitors an upstream Sentinel for master changes.
type SentinelMonitor struct {
	sentinelAddr string
	masterName   string
	auth         string

	mu            sync.RWMutex
	currentMaster string

	onChange func(oldMaster, newMaster string)

	stopOnce sync.Once
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// connMu protects subConn/pollConn so Stop() can close them to unblock reads
	connMu   sync.Mutex
	subConn  net.Conn
	pollConn net.Conn
}

func NewSentinelMonitor(sentinelAddr, masterName, auth string, onChange func(oldMaster, newMaster string)) *SentinelMonitor {
	return &SentinelMonitor{
		sentinelAddr: sentinelAddr,
		masterName:   masterName,
		auth:         auth,
		onChange:      onChange,
		stopCh:       make(chan struct{}),
	}
}

func (sm *SentinelMonitor) CurrentMaster() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentMaster
}

func (sm *SentinelMonitor) setMaster(addr string) {
	sm.mu.Lock()
	old := sm.currentMaster
	sm.currentMaster = addr
	sm.mu.Unlock()

	if old != addr {
		log.Printf("master changed: %s -> %s", old, addr)
		if sm.onChange != nil {
			sm.onChange(old, addr)
		}
	}
}

func (sm *SentinelMonitor) Start() {
	sm.wg.Add(2)
	go sm.pollLoop()
	go sm.subscribeLoop()
}

func (sm *SentinelMonitor) Stop() {
	sm.stopOnce.Do(func() {
		close(sm.stopCh)
		// Close active connections to unblock any pending reads
		sm.connMu.Lock()
		if sm.subConn != nil {
			sm.subConn.Close()
		}
		if sm.pollConn != nil {
			sm.pollConn.Close()
		}
		sm.connMu.Unlock()
	})
	sm.wg.Wait()
}

func (sm *SentinelMonitor) pollLoop() {
	defer sm.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// Poll immediately on start
	sm.pollOnce()

	for {
		select {
		case <-sm.stopCh:
			return
		case <-ticker.C:
			sm.pollOnce()
		}
	}
}

func (sm *SentinelMonitor) pollOnce() {
	addr, err := sm.queryMaster()
	if err != nil {
		log.Printf("sentinel poll error: %v", err)
		return
	}
	sm.setMaster(addr)
}

func (sm *SentinelMonitor) queryMaster() (string, error) {
	conn, err := net.DialTimeout("tcp", sm.sentinelAddr, 2*time.Second)
	if err != nil {
		return "", fmt.Errorf("dial sentinel: %w", err)
	}
	defer func() {
		sm.connMu.Lock()
		sm.pollConn = nil
		sm.connMu.Unlock()
		conn.Close()
	}()

	sm.connMu.Lock()
	select {
	case <-sm.stopCh:
		sm.connMu.Unlock()
		return "", fmt.Errorf("stopped")
	default:
	}
	sm.pollConn = conn
	sm.connMu.Unlock()

	conn.SetDeadline(time.Now().Add(3 * time.Second))

	reader := NewRESPReader(conn)
	writer := NewRESPWriter(conn)

	if sm.auth != "" {
		if err := writer.WriteBulkStringArray([]string{"AUTH", sm.auth}); err != nil {
			return "", fmt.Errorf("write AUTH: %w", err)
		}
		if err := writer.Flush(); err != nil {
			return "", fmt.Errorf("flush AUTH: %w", err)
		}
		resp, err := reader.ReadValue()
		if err != nil {
			return "", fmt.Errorf("read AUTH response: %w", err)
		}
		if resp.Type == TypeError {
			return "", fmt.Errorf("sentinel AUTH failed: %s", resp.Str)
		}
	}

	cmd := []string{"SENTINEL", "get-master-addr-by-name", sm.masterName}
	if err := writer.WriteBulkStringArray(cmd); err != nil {
		return "", fmt.Errorf("write command: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return "", fmt.Errorf("flush: %w", err)
	}

	resp, err := reader.ReadValue()
	if err != nil {
		return "", fmt.Errorf("read response: %w", err)
	}

	if resp.Type == TypeError {
		return "", fmt.Errorf("sentinel error: %s", resp.Str)
	}
	if resp.IsNil {
		return "", fmt.Errorf("master %q not found", sm.masterName)
	}
	if resp.Type != TypeArray || len(resp.Array) < 2 {
		return "", fmt.Errorf("unexpected response type or length")
	}

	host := resp.Array[0].Str
	port := resp.Array[1].Str
	return net.JoinHostPort(host, port), nil
}

func (sm *SentinelMonitor) subscribeLoop() {
	defer sm.wg.Done()

	for {
		select {
		case <-sm.stopCh:
			return
		default:
		}

		err := sm.subscribe()
		if err != nil {
			log.Printf("sentinel subscribe error: %v", err)
		}

		// Wait before reconnecting
		select {
		case <-sm.stopCh:
			return
		case <-time.After(1 * time.Second):
		}
	}
}

func (sm *SentinelMonitor) subscribe() error {
	conn, err := net.DialTimeout("tcp", sm.sentinelAddr, 2*time.Second)
	if err != nil {
		return fmt.Errorf("dial sentinel: %w", err)
	}
	defer func() {
		sm.connMu.Lock()
		sm.subConn = nil
		sm.connMu.Unlock()
		conn.Close()
	}()

	// Register conn so Stop() can close it to unblock reads.
	// Check stopCh first to bail out immediately if stop was called during dial.
	sm.connMu.Lock()
	select {
	case <-sm.stopCh:
		sm.connMu.Unlock()
		return nil
	default:
	}
	sm.subConn = conn
	sm.connMu.Unlock()

	// Set deadline for setup phase (AUTH + SUBSCRIBE ack)
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	reader := NewRESPReader(conn)
	writer := NewRESPWriter(conn)

	if sm.auth != "" {
		if err := writer.WriteBulkStringArray([]string{"AUTH", sm.auth}); err != nil {
			return fmt.Errorf("write AUTH: %w", err)
		}
		if err := writer.Flush(); err != nil {
			return fmt.Errorf("flush AUTH: %w", err)
		}
		resp, err := reader.ReadValue()
		if err != nil {
			return fmt.Errorf("read AUTH response: %w", err)
		}
		if resp.Type == TypeError {
			return fmt.Errorf("sentinel AUTH failed: %s", resp.Str)
		}
	}

	if err := writer.WriteBulkStringArray([]string{"SUBSCRIBE", "+switch-master"}); err != nil {
		return fmt.Errorf("write SUBSCRIBE: %w", err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	// Read and validate subscription acknowledgment
	ack, err := reader.ReadValue()
	if err != nil {
		return fmt.Errorf("read subscribe ack: %w", err)
	}
	if ack.Type == TypeError {
		return fmt.Errorf("subscribe rejected: %s", ack.Str)
	}
	if ack.Type != TypeArray || len(ack.Array) < 1 || strings.ToLower(ack.Array[0].Str) != "subscribe" {
		return fmt.Errorf("unexpected subscribe ack: type=%c", ack.Type)
	}

	// Clear deadline for message loop (we use per-read deadlines below)
	conn.SetDeadline(time.Time{})

	// Read messages until error or stop
	for {
		select {
		case <-sm.stopCh:
			return nil
		default:
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		msg, err := reader.ReadValue()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return fmt.Errorf("read message: %w", err)
		}

		// +switch-master message format: *3\r\n$7\r\nmessage\r\n$14\r\n+switch-master\r\n$...\r\n<payload>
		// Payload: "<master-name> <old-ip> <old-port> <new-ip> <new-port>"
		if msg.Type == TypeArray && len(msg.Array) >= 3 {
			if msg.Array[0].Str == "message" && msg.Array[1].Str == "+switch-master" {
				payload := msg.Array[2].Str
				parts := strings.Fields(payload)
				if len(parts) >= 5 && parts[0] == sm.masterName {
					newAddr := net.JoinHostPort(parts[3], parts[4])
					sm.setMaster(newAddr)
				}
			}
		}
	}
}
