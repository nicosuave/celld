# TCP container ingress

An ordinary TCP client reaches the container owned by the named `primary`
Durable Object. Docker or Podman and esbuild must be available.

From this directory:

```sh
celld dev --port 4544
```

In another terminal, `nc 127.0.0.1 4543` prints `READY` and echoes what you type.
The server can speak first; no HTTP, TLS, or custom client handshake is required.
`curl http://127.0.0.1:4544/status` reports the same object's ID and the count of
TCP startup hooks. Those counter writes pass through the normal durability gate.

The `tcp` entry in `wrangler.jsonc` exposes port 4543 and selects
`EchoContainer.getByName("primary")` on container port 7000. The startup hook
defaults to `/start-tcp`.

For a fleet, deploy this directory normally. Each node picks up the TCP mapping
with the deployment; no separate file or TCP launch flag is needed. TCP binds
the same IP as the node's HTTP listener, so configure `--listen` with a private
interface reachable from the load balancer. Forward raw TCP to port 4543 on any
healthy node. Internal addresses must also be mutually reachable. Redeploying
adds, changes, or removes listeners without restarting nodes; existing TCP
connections close and clients reconnect. An occupied port rejects that node's
adoption and leaves its previous deployment serving.

The startup hook probes an internal HTTP health port, which becomes ready only
after the TCP server binds. This avoids confusing Docker's published-port proxy
with application readiness on macOS.

The public endpoint stays fixed when ownership moves; established connections
close and clients reconnect. The startup hook owns container configuration and
must be idempotent. This example uses the native container API without an SDK
sleep alarm: celld pins the object while TCP is open, then resumes normal idle
eviction. An application alarm or explicit `destroy()` can still stop a busy
container.

TCP ingress is opt-in and does not authenticate clients or terminate TLS. Add
those at the load balancer or in the container protocol. Do not configure an
HTTP load balancer or add PROXY protocol bytes unless your container expects
them. Use the ordinary celld HTTP readiness endpoint for load-balancer health
checks; connecting to the TCP service itself activates its target.
