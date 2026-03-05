package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// RedisProxy proxies TCP connections to the current upstream Redis master.
type RedisProxy struct {
	listenAddr string
	auth       string
	getMaster  func() string

	listener net.Listener

	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	stopping bool

	wg sync.WaitGroup
}

func NewRedisProxy(listenAddr, auth string, getMaster func() string) *RedisProxy {
	return &RedisProxy{
		listenAddr: listenAddr,
		auth:       auth,
		getMaster:  getMaster,
		conns:      make(map[net.Conn]struct{}),
	}
}

func (p *RedisProxy) Start() error {
	ln, err := net.Listen("tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", p.listenAddr, err)
	}
	p.listener = ln
	log.Printf("redis proxy listening on %s", p.listenAddr)

	p.wg.Add(1)
	go p.acceptLoop()
	return nil
}

func (p *RedisProxy) Stop() {
	if p.listener != nil {
		p.listener.Close()
	}
	// Mark as stopping and close all tracked client connections to unblock io.Copy
	p.mu.Lock()
	p.stopping = true
	for conn := range p.conns {
		conn.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}

// trackConn registers a connection. Returns false if the proxy is stopping,
// in which case the connection is closed immediately.
func (p *RedisProxy) trackConn(conn net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		conn.Close()
		return false
	}
	p.conns[conn] = struct{}{}
	return true
}

func (p *RedisProxy) untrackConn(conn net.Conn) {
	p.mu.Lock()
	delete(p.conns, conn)
	p.mu.Unlock()
}

func (p *RedisProxy) acceptLoop() {
	defer p.wg.Done()

	var backoff time.Duration
	for {
		conn, err := p.listener.Accept()
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
			log.Printf("accept error (retrying in %v): %v", backoff, err)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.handleConn(conn)
		}()
	}
}

func (p *RedisProxy) writeError(client net.Conn, msg string) {
	w := NewRESPWriter(client)
	if err := w.WriteError(msg); err != nil {
		return
	}
	w.Flush()
}

func (p *RedisProxy) handleConn(client net.Conn) {
	if !p.trackConn(client) {
		return
	}
	defer func() {
		p.untrackConn(client)
		client.Close()
	}()

	master := p.getMaster()
	if master == "" {
		log.Printf("no master available, closing client connection")
		p.writeError(client, "ERR no master available")
		return
	}

	upstream, err := net.DialTimeout("tcp", master, 3*time.Second)
	if err != nil {
		log.Printf("dial upstream %s: %v", master, err)
		p.writeError(client, "ERR upstream unavailable")
		return
	}
	defer upstream.Close()

	// Authenticate with upstream if needed
	if p.auth != "" {
		reader := NewRESPReader(upstream)
		writer := NewRESPWriter(upstream)

		if err := writer.WriteBulkStringArray([]string{"AUTH", p.auth}); err != nil {
			log.Printf("upstream AUTH write: %v", err)
			p.writeError(client, "ERR upstream auth failed")
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("upstream AUTH flush: %v", err)
			p.writeError(client, "ERR upstream auth failed")
			return
		}

		resp, err := reader.ReadValue()
		if err != nil {
			log.Printf("upstream AUTH read: %v", err)
			p.writeError(client, "ERR upstream auth failed")
			return
		}
		if resp.Type == TypeError {
			log.Printf("upstream AUTH failed: %s", resp.Str)
			p.writeError(client, "ERR upstream auth failed")
			return
		}
	}

	// Bidirectional copy
	var once sync.Once
	done := make(chan struct{})
	closeConns := func() {
		client.Close()
		upstream.Close()
		close(done)
	}

	go func() {
		io.Copy(upstream, client)
		once.Do(closeConns)
	}()

	io.Copy(client, upstream)
	once.Do(closeConns)
	<-done
}
