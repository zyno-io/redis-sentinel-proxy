package main

import (
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
	wg       sync.WaitGroup
}

func NewRedisProxy(listenAddr, auth string, getMaster func() string) *RedisProxy {
	return &RedisProxy{
		listenAddr: listenAddr,
		auth:       auth,
		getMaster:  getMaster,
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
	p.wg.Wait()
}

func (p *RedisProxy) acceptLoop() {
	defer p.wg.Done()

	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return // listener closed
		}
		go p.handleConn(conn)
	}
}

func (p *RedisProxy) handleConn(client net.Conn) {
	defer client.Close()

	master := p.getMaster()
	if master == "" {
		log.Printf("no master available, closing client connection")
		return
	}

	upstream, err := net.DialTimeout("tcp", master, 3*time.Second)
	if err != nil {
		log.Printf("dial upstream %s: %v", master, err)
		return
	}
	defer upstream.Close()

	// Authenticate with upstream if needed
	if p.auth != "" {
		reader := NewRESPReader(upstream)
		writer := NewRESPWriter(upstream)

		if err := writer.WriteBulkStringArray([]string{"AUTH", p.auth}); err != nil {
			log.Printf("upstream AUTH write: %v", err)
			return
		}
		if err := writer.Flush(); err != nil {
			log.Printf("upstream AUTH flush: %v", err)
			return
		}

		resp, err := reader.ReadValue()
		if err != nil {
			log.Printf("upstream AUTH read: %v", err)
			return
		}
		if resp.Type == TypeError {
			log.Printf("upstream AUTH failed: %s", resp.Str)
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
