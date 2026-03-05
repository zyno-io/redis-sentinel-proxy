package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	listenAddr := flag.String("listen", envOrDefault("LISTEN", ":6379"), "Redis proxy listen address")
	sentinelListenAddr := flag.String("sentinel-listen", envOrDefault("SENTINEL_LISTEN", ":26379"), "Sentinel emulator listen address")
	sentinelAddr := flag.String("sentinel", envOrDefault("SENTINEL", ":26379"), "Upstream Sentinel address")
	masterName := flag.String("master", envOrDefault("MASTER", ""), "Sentinel master name (required)")
	auth := flag.String("auth", envOrDefault("AUTH", ""), "Password for upstream Redis master")
	announceIP := flag.String("announce-ip", envOrDefault("ANNOUNCE_IP", ""), "IP to advertise as master (auto-detect if empty)")
	announcePort := flag.String("announce-port", envOrDefault("ANNOUNCE_PORT", ""), "Port to advertise as master (from -listen if empty)")
	flag.Parse()

	if *masterName == "" {
		fmt.Fprintln(os.Stderr, "error: -master flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Auto-detect announce IP
	if *announceIP == "" {
		ip, err := detectOutboundIP()
		if err != nil {
			log.Fatalf("failed to auto-detect IP: %v (use -announce-ip to set manually)", err)
		}
		*announceIP = ip
		log.Printf("auto-detected announce IP: %s", ip)
	}

	// Derive announce port from listen address if not specified
	if *announcePort == "" {
		_, port, err := net.SplitHostPort(*listenAddr)
		if err != nil {
			log.Fatalf("invalid -listen address %q: %v", *listenAddr, err)
		}
		*announcePort = port
	}

	// Create sentinel emulator
	emulator := NewSentinelEmulator(*sentinelListenAddr, *masterName, *announceIP, *announcePort)

	// Create sentinel monitor with onChange callback that broadcasts to subscribers
	monitor := NewSentinelMonitor(*sentinelAddr, *masterName, *auth, func(newMaster string) {
		newHost, newPort, _ := net.SplitHostPort(newMaster)
		emulator.BroadcastSwitchMaster("0.0.0.0", "0", newHost, newPort)
	})

	// Create redis proxy
	proxy := NewRedisProxy(*listenAddr, *auth, monitor.CurrentMaster)

	// Start components
	monitor.Start()

	if err := emulator.Start(); err != nil {
		log.Fatalf("sentinel emulator: %v", err)
	}
	if err := proxy.Start(); err != nil {
		log.Fatalf("redis proxy: %v", err)
	}

	log.Printf("redis-proxy started (master=%s, sentinel=%s, announce=%s:%s)",
		*masterName, *sentinelAddr, *announceIP, *announcePort)

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("shutting down...")
	proxy.Stop()
	emulator.Stop()
	monitor.Stop()
	log.Println("stopped")
}

func envOrDefault(key, defaultVal string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return defaultVal
}

func detectOutboundIP() (string, error) {
	conn, err := net.Dial("udp", "8.8.8.8:53")
	if err != nil {
		return "", err
	}
	defer conn.Close()
	addr := conn.LocalAddr().(*net.UDPAddr)
	ip := addr.IP.String()
	if strings.HasPrefix(ip, "127.") {
		return "", fmt.Errorf("detected loopback address %s", ip)
	}
	return ip, nil
}
