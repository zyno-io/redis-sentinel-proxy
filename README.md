# redis-sentinel-proxy

A lightweight TCP proxy that allows [ioredis](https://github.com/redis/ioredis) clients in one Kubernetes cluster to connect to Redis Sentinel-managed hosts in another cluster through a LoadBalancerIP.

The proxy runs in the primary cluster alongside Redis and Sentinel. It monitors the upstream Sentinel for failovers, proxies Redis traffic to the current master, and exposes a Sentinel emulator that ioredis clients in remote clusters can use for discovery and failover notifications.

## Use Case

This proxy is designed for **trusted, internal traffic** between Kubernetes clusters. It is not intended to be exposed to the public internet or to handle untrusted clients.

The only supported downstream clients are **ioredis** instances using Sentinel-based connection. The Sentinel emulator implements the minimal subset of the Sentinel protocol that ioredis requires:

- `SENTINEL get-master-addr-by-name` — master address discovery
- `SENTINEL masters` — master listing
- `SUBSCRIBE +switch-master` — failover notifications
- `AUTH`, `PING`, `INFO`, `CLIENT` — connection lifecycle

Authentication uses password-only `AUTH` (not ACL username+password).

## How It Works

redis-proxy has three components:

1. **Sentinel Monitor** — Connects to an upstream Redis Sentinel, polls for the current master, and subscribes to `+switch-master` notifications for instant failover detection.

2. **Redis Proxy** — Accepts TCP connections from Redis clients and proxies them to the current upstream master. Handles upstream authentication transparently.

3. **Sentinel Emulator** — Listens on a Sentinel port and responds to Sentinel protocol commands, directing clients back to the proxy's own address (the LoadBalancerIP).

When a failover occurs, the monitor detects the new master and the emulator broadcasts a `+switch-master` message to all subscribed ioredis clients, triggering reconnection through the proxy.

## Deployment

Deploy redis-proxy in the same cluster as your Redis Sentinel. Expose it via a Kubernetes Service of type `LoadBalancer` on both the Redis port (6379) and the Sentinel emulator port (26379). Set `-announce-ip` to the LoadBalancerIP so that ioredis clients in remote clusters discover the correct address.

```yaml
# Example: ioredis connection config in the remote cluster
const redis = new Redis({
  sentinels: [{ host: '<load-balancer-ip>', port: 26379 }],
  name: 'mymaster',
});
```

## Usage

```bash
redis-sentinel-proxy \
  -master mymaster \
  -announce-ip 10.0.0.50 \
  -sentinel 10.0.0.1:26379 \
  -auth redis-password \
  -sentinel-auth sentinel-password
```

## Configuration

All flags can also be set via environment variables.

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `-listen` | `LISTEN` | `:6379` | Redis proxy listen address |
| `-sentinel-listen` | `SENTINEL_LISTEN` | `:26379` | Sentinel emulator listen address |
| `-sentinel` | `SENTINEL` | `:26379` | Upstream Sentinel address |
| `-master` | `MASTER` | *(required)* | Sentinel master name |
| `-auth` | `AUTH` | *(empty)* | Password for upstream Redis master |
| `-sentinel-auth` | `SENTINEL_AUTH` | *(defaults to `-auth`)* | Password for upstream Sentinel |
| `-announce-ip` | `ANNOUNCE_IP` | *(required)* | IP to advertise as master to downstream clients |
| `-announce-port` | `ANNOUNCE_PORT` | *(from `-listen`)* | Port to advertise as master to downstream clients |

## Limitations

- **Password-only auth**: Uses single-argument `AUTH <password>`. ACL username+password (`AUTH <user> <pass>`) is not supported.
- **ioredis only**: The Sentinel emulator implements just enough of the Sentinel protocol for ioredis. Other clients may not work.
- **Trusted traffic**: No TLS, rate limiting, or client authentication on the proxy/emulator ports. Intended for internal cluster-to-cluster networking only.

## Building

```bash
go build -o redis-sentinel-proxy .
```

## Testing

```bash
go test -race ./...
```
