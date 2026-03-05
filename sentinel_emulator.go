package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// SentinelEmulator emulates a Redis Sentinel for downstream clients.
type SentinelEmulator struct {
	listenAddr   string
	masterName   string
	announceIP   string
	announcePort string

	listener net.Listener

	mu          sync.Mutex
	conns       map[net.Conn]struct{}
	subscribers map[net.Conn]*sync.Mutex // per-conn write mutex
	stopping    bool

	wg sync.WaitGroup
}

func NewSentinelEmulator(listenAddr, masterName, announceIP, announcePort string) *SentinelEmulator {
	return &SentinelEmulator{
		listenAddr:   listenAddr,
		masterName:   masterName,
		announceIP:   announceIP,
		announcePort: announcePort,
		conns:        make(map[net.Conn]struct{}),
		subscribers:  make(map[net.Conn]*sync.Mutex),
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

	// Mark as stopping and close all tracked connections
	se.mu.Lock()
	se.stopping = true
	for conn := range se.conns {
		conn.Close()
	}
	se.mu.Unlock()

	se.wg.Wait()
}

// trackConn registers a connection. Returns false if the emulator is stopping,
// in which case the connection is closed immediately.
func (se *SentinelEmulator) trackConn(conn net.Conn) bool {
	se.mu.Lock()
	defer se.mu.Unlock()
	if se.stopping {
		conn.Close()
		return false
	}
	se.conns[conn] = struct{}{}
	return true
}

func (se *SentinelEmulator) untrackConn(conn net.Conn) {
	se.mu.Lock()
	delete(se.conns, conn)
	delete(se.subscribers, conn)
	se.mu.Unlock()
}

func (se *SentinelEmulator) acceptLoop() {
	defer se.wg.Done()

	var backoff time.Duration
	for {
		conn, err := se.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else {
				backoff *= 2
				if backoff > time.Second {
					backoff = time.Second
				}
			}
			log.Printf("sentinel accept error (retrying in %v): %v", backoff, err)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		if !se.trackConn(conn) {
			continue
		}
		se.wg.Add(1)
		go se.handleConn(conn)
	}
}

func (se *SentinelEmulator) handleConn(conn net.Conn) {
	defer func() {
		se.untrackConn(conn)
		conn.Close()
		se.wg.Done()
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

		var cmdErr error
		switch cmd {
		case "PING":
			if err := writer.WriteSimpleString("PONG"); err != nil {
				return
			}
			cmdErr = writer.Flush()

		case "AUTH":
			// Accept any auth for the emulator
			if err := writer.WriteSimpleString("OK"); err != nil {
				return
			}
			cmdErr = writer.Flush()

		case "SENTINEL":
			cmdErr = se.handleSentinelCmd(writer, args[1:])

		case "SUBSCRIBE":
			se.handleSubscribe(conn, reader, writer, args[1:])
			return // connection is now in pub/sub mode

		case "INFO":
			cmdErr = se.handleInfo(writer, args[1:])

		case "CLIENT":
			// Handle CLIENT SETNAME, CLIENT GETNAME, etc.
			if err := writer.WriteSimpleString("OK"); err != nil {
				return
			}
			cmdErr = writer.Flush()

		default:
			if err := writer.WriteError("ERR unknown command '" + args[0] + "'"); err != nil {
				return
			}
			cmdErr = writer.Flush()
		}

		if cmdErr != nil {
			return
		}
	}
}

func (se *SentinelEmulator) handleSentinelCmd(writer *RESPWriter, args []string) error {
	if len(args) == 0 {
		writer.WriteError("ERR wrong number of arguments for 'sentinel' command")
		return writer.Flush()
	}

	subcmd := strings.ToUpper(args[0])

	switch subcmd {
	case "GET-MASTER-ADDR-BY-NAME":
		if len(args) < 2 {
			writer.WriteError("ERR wrong number of arguments for 'sentinel get-master-addr-by-name' command")
			return writer.Flush()
		}
		if args[1] != se.masterName {
			writer.WriteNullArray()
			return writer.Flush()
		}
		writer.WriteBulkStringArray([]string{se.announceIP, se.announcePort})
		return writer.Flush()

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
		return writer.Flush()

	case "SENTINELS":
		// Return empty array
		writer.WriteArrayHeader(0)
		return writer.Flush()

	case "SLAVES", "REPLICAS":
		// Return empty array
		writer.WriteArrayHeader(0)
		return writer.Flush()

	default:
		writer.WriteError("ERR NOTIMPLEMENTED sentinel subcommand '" + args[0] + "'")
		return writer.Flush()
	}
}

func (se *SentinelEmulator) handleSubscribe(conn net.Conn, reader *RESPReader, writer *RESPWriter, channels []string) {
	if len(channels) == 0 {
		writer.WriteError("ERR wrong number of arguments for 'subscribe' command")
		writer.Flush()
		return
	}

	for i, ch := range channels {
		writer.WriteArrayHeader(3)
		writer.WriteBulkString("subscribe")
		writer.WriteBulkString(ch)
		writer.WriteInteger(int64(i + 1))
		if err := writer.Flush(); err != nil {
			return
		}
	}

	// Register as subscriber with a per-connection write mutex
	writeMu := &sync.Mutex{}
	se.mu.Lock()
	se.subscribers[conn] = writeMu
	se.mu.Unlock()

	// Keep connection alive - read until error (reuse existing reader to avoid losing buffered data)
	for {
		val, err := reader.ReadValue()
		if err != nil {
			return
		}

		// Handle PING in pub/sub mode: respond with ["pong", <msg>]
		args := val.ToArgs()
		if len(args) > 0 && strings.ToUpper(args[0]) == "PING" {
			msg := ""
			if len(args) > 1 {
				msg = args[1]
			}
			writeMu.Lock()
			writer.WriteArrayHeader(2)
			writer.WriteBulkString("pong")
			writer.WriteBulkString(msg)
			err = writer.Flush()
			writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func (se *SentinelEmulator) handleInfo(writer *RESPWriter, args []string) error {
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
	return writer.Flush()
}

// BroadcastSwitchMaster sends a +switch-master message to all subscribed clients.
// oldMaster is the previous upstream master address (host:port). The "new" fields
// use the proxy's announce address so clients reconnect through the proxy.
func (se *SentinelEmulator) BroadcastSwitchMaster(oldMaster string) {
	oldHost, oldPort, err := net.SplitHostPort(oldMaster)
	if err != nil || oldHost == "" {
		oldHost = "0.0.0.0"
		oldPort = "0"
	}
	payload := fmt.Sprintf("%s %s %s %s %s",
		se.masterName, oldHost, oldPort, se.announceIP, se.announcePort)

	se.mu.Lock()
	type sub struct {
		conn    net.Conn
		writeMu *sync.Mutex
	}
	subs := make([]sub, 0, len(se.subscribers))
	for conn, mu := range se.subscribers {
		subs = append(subs, sub{conn, mu})
	}
	se.mu.Unlock()

	for _, s := range subs {
		s.writeMu.Lock()
		s.conn.SetWriteDeadline(time.Now().Add(3 * time.Second))
		writer := NewRESPWriter(s.conn)
		writer.WriteArrayHeader(3)
		writer.WriteBulkString("message")
		writer.WriteBulkString("+switch-master")
		writer.WriteBulkString(payload)
		err := writer.Flush()
		s.conn.SetWriteDeadline(time.Time{})
		s.writeMu.Unlock()

		if err != nil {
			s.conn.Close()
			se.mu.Lock()
			delete(se.subscribers, s.conn)
			se.mu.Unlock()
		}
	}
}
