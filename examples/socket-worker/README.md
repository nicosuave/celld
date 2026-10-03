# Socket Worker echo

Run `celld dev examples/socket-worker` from the repository root, then connect
with `nc 127.0.0.1 5432`. Bytes written to the connection are echoed back.

For a fleet, deploy with `celld deploy examples/socket-worker` using the
fleet's normal bucket configuration.

`socket_workers` is celld-specific deployment configuration. Its nonzero TCP
ports listen on the same IP address as the node's HTTP listener. Each accepted
connection calls the primary Worker's `connect(socket, env, ctx)` handler.
This replaces the Spectrum-to-Worker routing used on Cloudflare; it does not
create a container or expose a named container port. Service-binding targets
do not open listeners on the node.

The handler receives a Socket with `readable`, `writable`, `opened`, `closed`,
and `close()`. Await stream processing, as this example does, or register it
with `ctx.waitUntil()` so the connection's event remains alive. It returns
void, rather than an HTTP Response.

Listener configuration is versioned with the deployment. New ports must bind
successfully before the node adopts the new generation. Removing a port closes
its listener and connections; shutting down the node also closes connections.
The listener carries raw TCP. Put any required TLS termination and network
access controls in front of it.

To route through a Durable Object, declare a `SOCKET_DO` binding and a SQLite
migration for `SocketDurableObject`, then use this Worker module:

```js
import { DurableObject } from "cloudflare:workers";

async function relay(source, destination) {
  await Promise.all([
    source.readable.pipeTo(destination.writable),
    destination.readable.pipeTo(source.writable),
  ]);
}

export class SocketDurableObject extends DurableObject {
  async connect(socket) {
    await socket.readable.pipeTo(socket.writable);
  }
}

export default {
  async connect(socket, env) {
    const destination = env.SOCKET_DO.getByName("my-server").connect("host:5432");
    await relay(socket, destination);
  },
};
```

A service binding works the same way: call `env.SERVICE.connect("host:5432")`
and relay to its Socket. Its Worker implements its own `connect` handler.
The address describes the connection; the binding selects the destination.
Durable Object connections follow celld's existing owner routing, including
an authenticated peer connection when the object belongs to another node.

For a container-backed Durable Object, start the container and wait for its
program to listen, then relay to
`this.ctx.container.getTcpPort(8080).connect("10.0.0.1:8080")`. The configured
container port selects the actual destination, as with `getTcpPort().fetch()`.
See [Containers](../../docs/services/containers.md) for image configuration and
startup behavior. The binary integration suite covers this full path with a
Docker echo server (`cargo test -p celld --test socket_workers tcp_worker_forwards
-- --ignored`).

These APIs follow the [Cloudflare Socket Workers announcement](https://blog.cloudflare.com/grpc-workers/).
