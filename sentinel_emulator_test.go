package main

import (
	"net"
	"strings"
	"testing"
	"time"
)

func startTestEmulator(t *testing.T) (*SentinelEmulator, string) {
	t.Helper()
	se := NewSentinelEmulator("127.0.0.1:0", "mymaster", "10.0.0.1", "6379")
	if err := se.Start(); err != nil {
		t.Fatal(err)
	}
	addr := se.listener.Addr().String()
	t.Cleanup(func() { se.Stop() })
	return se, addr
}

func dialEmulator(t *testing.T, addr string) (net.Conn, *RESPReader, *RESPWriter) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn, NewRESPReader(conn), NewRESPWriter(conn)
}

func sendCommand(t *testing.T, w *RESPWriter, args ...string) {
	t.Helper()
	if err := w.WriteBulkStringArray(args); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestEmulatorPing(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "PING")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeSimpleString || val.Str != "PONG" {
		t.Fatalf("expected +PONG, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorAuth(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "AUTH", "somepassword")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeSimpleString || val.Str != "OK" {
		t.Fatalf("expected +OK, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorGetMasterAddrByName(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "get-master-addr-by-name", "mymaster")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 2 {
		t.Fatalf("expected array of 2, got %c len=%d", val.Type, len(val.Array))
	}
	if val.Array[0].Str != "10.0.0.1" {
		t.Fatalf("expected IP 10.0.0.1, got %q", val.Array[0].Str)
	}
	if val.Array[1].Str != "6379" {
		t.Fatalf("expected port 6379, got %q", val.Array[1].Str)
	}
}

func TestEmulatorGetMasterAddrByNameUnknown(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "get-master-addr-by-name", "unknown")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || !val.IsNil {
		t.Fatalf("expected null array for unknown master, got type=%c nil=%v", val.Type, val.IsNil)
	}
}

func TestEmulatorMasters(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "masters")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 1 {
		t.Fatalf("expected outer array of 1, got %d", len(val.Array))
	}
	inner := val.Array[0]
	if inner.Type != TypeArray {
		t.Fatal("expected inner array")
	}
	// Check key-value pairs
	m := make(map[string]string)
	for i := 0; i+1 < len(inner.Array); i += 2 {
		m[inner.Array[i].Str] = inner.Array[i+1].Str
	}
	if m["name"] != "mymaster" {
		t.Fatalf("expected name=mymaster, got %q", m["name"])
	}
	if m["ip"] != "10.0.0.1" {
		t.Fatalf("expected ip=10.0.0.1, got %q", m["ip"])
	}
	if m["port"] != "6379" {
		t.Fatalf("expected port=6379, got %q", m["port"])
	}
}

func TestEmulatorSentinels(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "sentinels", "mymaster")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 0 {
		t.Fatalf("expected empty array, got %d", len(val.Array))
	}
}

func TestEmulatorSlaves(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "slaves", "mymaster")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 0 {
		t.Fatalf("expected empty array, got %d", len(val.Array))
	}
}

func TestEmulatorReplicas(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "replicas", "mymaster")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 0 {
		t.Fatalf("expected empty array, got %d", len(val.Array))
	}
}

func TestEmulatorInfo(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "INFO")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString {
		t.Fatalf("expected bulk string, got %c", val.Type)
	}
	if !strings.Contains(val.Str, "sentinel_masters:1") {
		t.Fatalf("expected sentinel info, got %q", val.Str)
	}
	if !strings.Contains(val.Str, "mymaster") {
		t.Fatalf("expected master name in info, got %q", val.Str)
	}
}

func TestEmulatorInfoSentinelSection(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "INFO", "sentinel")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString || !strings.Contains(val.Str, "# Sentinel") {
		t.Fatalf("expected sentinel info section, got %q", val.Str)
	}
}

func TestEmulatorInfoUnknownSection(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "INFO", "replication")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeBulkString || val.Str != "" {
		t.Fatalf("expected empty bulk string for unknown section, got %q", val.Str)
	}
}

func TestEmulatorClientCommand(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "CLIENT", "SETNAME", "test-client")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeSimpleString || val.Str != "OK" {
		t.Fatalf("expected +OK, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorUnknownCommand(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "FLUSHALL")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeError {
		t.Fatalf("expected error, got %c %q", val.Type, val.Str)
	}
	if !strings.Contains(val.Str, "unknown command") {
		t.Fatalf("expected unknown command error, got %q", val.Str)
	}
}

func TestEmulatorUnknownSentinelSubcommand(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "failover", "mymaster")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeError {
		t.Fatalf("expected error, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorSentinelNoSubcommand(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeError {
		t.Fatalf("expected error, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorGetMasterAddrByNameNoName(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	sendCommand(t, w, "SENTINEL", "get-master-addr-by-name")

	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeError {
		t.Fatalf("expected error for missing master name, got %c %q", val.Type, val.Str)
	}
}

func TestEmulatorSubscribe(t *testing.T) {
	_, addr := startTestEmulator(t)
	conn, r, w := dialEmulator(t, addr)
	_ = conn

	sendCommand(t, w, "SUBSCRIBE", "+switch-master")

	// Read subscription acknowledgment
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 3 {
		t.Fatalf("expected subscribe ack array of 3, got %d", len(val.Array))
	}
	if val.Array[0].Str != "subscribe" {
		t.Fatalf("expected 'subscribe', got %q", val.Array[0].Str)
	}
	if val.Array[1].Str != "+switch-master" {
		t.Fatalf("expected '+switch-master', got %q", val.Array[1].Str)
	}
	if val.Array[2].Int != 1 {
		t.Fatalf("expected subscription count 1, got %d", val.Array[2].Int)
	}
}

func TestEmulatorBroadcastSwitchMaster(t *testing.T) {
	se, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	// Subscribe
	sendCommand(t, w, "SUBSCRIBE", "+switch-master")
	// Read ack
	if _, err := r.ReadValue(); err != nil {
		t.Fatal(err)
	}

	// Give the subscriber goroutine time to register
	time.Sleep(50 * time.Millisecond)

	// Broadcast a switch-master event
	se.BroadcastSwitchMaster("10.0.0.1", "6379", "10.0.0.2", "6380")

	// Read the broadcast message
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || len(val.Array) != 3 {
		t.Fatalf("expected message array of 3, got type=%c len=%d", val.Type, len(val.Array))
	}
	if val.Array[0].Str != "message" {
		t.Fatalf("expected 'message', got %q", val.Array[0].Str)
	}
	if val.Array[1].Str != "+switch-master" {
		t.Fatalf("expected '+switch-master', got %q", val.Array[1].Str)
	}
	payload := val.Array[2].Str
	if !strings.Contains(payload, "mymaster") {
		t.Fatalf("expected master name in payload, got %q", payload)
	}
	if !strings.Contains(payload, "10.0.0.2") {
		t.Fatalf("expected new IP in payload, got %q", payload)
	}
	if !strings.Contains(payload, "6380") {
		t.Fatalf("expected new port in payload, got %q", payload)
	}
}

func TestEmulatorMultipleCommands(t *testing.T) {
	_, addr := startTestEmulator(t)
	_, r, w := dialEmulator(t, addr)

	// Send multiple commands on the same connection
	sendCommand(t, w, "PING")
	val, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Str != "PONG" {
		t.Fatalf("expected PONG, got %q", val.Str)
	}

	sendCommand(t, w, "SENTINEL", "get-master-addr-by-name", "mymaster")
	val, err = r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if val.Type != TypeArray || val.Array[0].Str != "10.0.0.1" {
		t.Fatalf("expected master addr, got %v", val)
	}

	sendCommand(t, w, "INFO", "sentinel")
	val, err = r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(val.Str, "sentinel_masters") {
		t.Fatalf("expected info, got %q", val.Str)
	}
}
