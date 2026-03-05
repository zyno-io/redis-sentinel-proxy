package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	listenAddr := flag.String("listen", envOrDefault("LISTEN", ":6379"), "Redis proxy listen address")
	sentinelListenAddr := flag.String("sentinel-listen", envOrDefault("SENTINEL_LISTEN", ":26379"), "Sentinel emulator listen address")
	sentinelAddr := flag.String("sentinel", envOrDefault("SENTINEL", "127.0.0.1:26379"), "Upstream Sentinel address")
	masterName := flag.String("master", envOrDefault("MASTER", ""), "Sentinel master name (required)")
	auth := flag.String("auth", envOrDefault("AUTH", ""), "Password for upstream Redis master")
	sentinelAuth := flag.String("sentinel-auth", envOrDefault("SENTINEL_AUTH", ""), "Password for upstream Sentinel (defaults to -auth if empty)")
	announceIP := flag.String("announce-ip", envOrDefault("ANNOUNCE_IP", ""), "IP to advertise as master (required)")
	announcePort := flag.String("announce-port", envOrDefault("ANNOUNCE_PORT", ""), "Port to advertise as master (from -listen if empty)")
	flag.Parse()

	if *masterName == "" {
		fmt.Fprintln(os.Stderr, "error: -master flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Default sentinel auth to redis auth if not explicitly set via flag or env
	sentinelAuthSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "sentinel-auth" {
			sentinelAuthSet = true
		}
	})
	if !sentinelAuthSet {
		if _, ok := os.LookupEnv("SENTINEL_AUTH"); !ok {
			*sentinelAuth = *auth
		}
	}

	if *announceIP == "" {
		fmt.Fprintln(os.Stderr, "error: -announce-ip flag is required")
		flag.Usage()
		os.Exit(1)
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
	monitor := NewSentinelMonitor(*sentinelAddr, *masterName, *sentinelAuth, func(oldMaster, newMaster string) {
		emulator.BroadcastSwitchMaster(oldMaster)
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
