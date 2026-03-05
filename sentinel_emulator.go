package main

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
)

// SentinelEmulator emulates a Redis Sentinel for downstream clients.
type SentinelEmulator struct {
	listenAddr   string
	masterName   string
	announceIP   string
	announcePort string

	listener net.Listener

	mu          sync.Mutex
	subscribers map[net.Conn]struct{}

	wg sync.WaitGroup
}

func NewSentinelEmulator(listenAddr, masterName, announceIP, announcePort string) *SentinelEmulator {
	return &SentinelEmulator{
		listenAddr:   listenAddr,
		masterName:   masterName,
		announceIP:   announceIP,
		announcePort: announcePort,
		subscribers:  make(map[net.Conn]struct{}),
	}
}

func (se *SentinelEmulator) Start() error {
	ln, err := net.Listen("tcp", se.listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", se.listenAddr, err)
	}
	se.listener = ln
	log.Printf("sentinel emulator listening on %s", se.listenAddr)

	se.wg.Add(1)
	go se.acceptLoop()
	return nil
}

func (se *SentinelEmulator) Stop() {
	if se.listener != nil {
		se.listener.Close()
	}

	se.mu.Lock()
	for conn := range se.subscribers {
		conn.Close()
	}
	se.mu.Unlock()

	se.wg.Wait()
}

func (se *SentinelEmulator) acceptLoop() {
	defer se.wg.Done()

	for {
		conn, err := se.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go se.handleConn(conn)
	}
}

func (se *SentinelEmulator) handleConn(conn net.Conn) {
	defer func() {
		se.mu.Lock()
		delete(se.subscribers, conn)
		se.mu.Unlock()
		conn.Close()
	}()

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
		case "PING":
			writer.WriteSimpleString("PONG")
			writer.Flush()

		case "AUTH":
			// Accept any auth for the emulator
			writer.WriteSimpleString("OK")
			writer.Flush()

		case "SENTINEL":
			se.handleSentinelCmd(writer, args[1:])

		case "SUBSCRIBE":
			se.handleSubscribe(conn, writer, args[1:])
			return // connection is now in pub/sub mode

		case "INFO":
			se.handleInfo(writer, args[1:])

		case "CLIENT":
			// Handle CLIENT SETNAME, CLIENT GETNAME, etc.
			writer.WriteSimpleString("OK")
			writer.Flush()

		default:
			writer.WriteError("ERR unknown command '" + args[0] + "'")
			writer.Flush()
		}
	}
}

func (se *SentinelEmulator) handleSentinelCmd(writer *RESPWriter, args []string) {
	if len(args) == 0 {
		writer.WriteError("ERR wrong number of arguments for 'sentinel' command")
		writer.Flush()
		return
	}

	subcmd := strings.ToUpper(args[0])

	switch subcmd {
	case "GET-MASTER-ADDR-BY-NAME":
		if len(args) < 2 {
			writer.WriteError("ERR wrong number of arguments for 'sentinel get-master-addr-by-name' command")
			writer.Flush()
			return
		}
		if args[1] != se.masterName {
			writer.WriteNullArray()
			writer.Flush()
			return
		}
		writer.WriteBulkStringArray([]string{se.announceIP, se.announcePort})
		writer.Flush()

	case "MASTERS":
		// Return a single master entry
		masterInfo := []string{
			"name", se.masterName,
			"ip", se.announceIP,
			"port", se.announcePort,
			"flags", "master",
			"num-slaves", "0",
			"num-other-sentinels", "0",
			"quorum", "1",
		}
		writer.WriteArrayHeader(1)
		writer.WriteBulkStringArray(masterInfo)
		writer.Flush()

	case "SENTINELS":
		// Return empty array
		writer.WriteArrayHeader(0)
		writer.Flush()

	case "SLAVES", "REPLICAS":
		// Return empty array
		writer.WriteArrayHeader(0)
		writer.Flush()

	default:
		writer.WriteError("ERR Unknown sentinel subcommand '" + args[0] + "'")
		writer.Flush()
	}
}

func (se *SentinelEmulator) handleSubscribe(conn net.Conn, writer *RESPWriter, channels []string) {
	for i, ch := range channels {
		writer.WriteArrayHeader(3)
		writer.WriteBulkString("subscribe")
		writer.WriteBulkString(ch)
		writer.WriteInteger(int64(i + 1))
		writer.Flush()
	}

	// Register as subscriber
	se.mu.Lock()
	se.subscribers[conn] = struct{}{}
	se.mu.Unlock()

	// Keep connection alive - read until error
	reader := NewRESPReader(conn)
	for {
		_, err := reader.ReadValue()
		if err != nil {
			return
		}
	}
}

func (se *SentinelEmulator) handleInfo(writer *RESPWriter, args []string) {
	section := ""
	if len(args) > 0 {
		section = strings.ToLower(args[0])
	}

	info := ""
	if section == "" || section == "sentinel" || section == "server" {
		info = "# Sentinel\r\n" +
			"sentinel_masters:1\r\n" +
			"sentinel_running_scripts:0\r\n" +
			"sentinel_scripts_queue_length:0\r\n" +
			fmt.Sprintf("master0:name=%s,status=ok,address=%s:%s,slaves=0,sentinels=1\r\n",
				se.masterName, se.announceIP, se.announcePort)
	}

	writer.WriteBulkString(info)
	writer.Flush()
}

// BroadcastSwitchMaster sends a +switch-master message to all subscribed clients.
func (se *SentinelEmulator) BroadcastSwitchMaster(oldIP, oldPort, newIP, newPort string) {
	payload := fmt.Sprintf("%s %s %s %s %s", se.masterName, oldIP, oldPort, newIP, newPort)

	se.mu.Lock()
	subscribers := make([]net.Conn, 0, len(se.subscribers))
	for conn := range se.subscribers {
		subscribers = append(subscribers, conn)
	}
	se.mu.Unlock()

	for _, conn := range subscribers {
		writer := NewRESPWriter(conn)
		writer.WriteArrayHeader(3)
		writer.WriteBulkString("message")
		writer.WriteBulkString("+switch-master")
		writer.WriteBulkString(payload)
		if err := writer.Flush(); err != nil {
			conn.Close()
			se.mu.Lock()
			delete(se.subscribers, conn)
			se.mu.Unlock()
		}
	}
}
