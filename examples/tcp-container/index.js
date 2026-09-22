import { DurableObject } from "cloudflare:workers";

export class EchoContainer extends DurableObject {
  async fetch(request) {
    const path = new URL(request.url).pathname;
    if (request.method === "POST" && path === "/start-tcp") {
      // Called for every TCP connection. Keep this hook idempotent; celld
      // waits for the port and gates these storage writes before forwarding.
      if (!this.ctx.container.running) {
        this.ctx.container.start({ enableInternet: false });
      }
      // start() is asynchronous. A real application response is stronger
      // than a connection to Docker's published-port proxy on macOS.
      const deadline = Date.now() + 20_000;
      let ready = false;
      while (Date.now() < deadline) {
        try {
          const response = await this.ctx.container.getTcpPort(7001).fetch("http://container/ready");
          if (response.status === 204) {
            ready = true;
            break;
          }
          await response.body?.cancel();
        } catch {
          // The application has not bound its health port yet.
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      if (!ready) return new Response("Container did not become ready", { status: 503 });
      const connections = (await this.ctx.storage.get("connections")) ?? 0;
      await this.ctx.storage.put("connections", connections + 1);
      return new Response(null, { status: 204 });
    }
    if (path === "/status") {
      return Response.json({
        id: this.ctx.id.toString(),
        running: this.ctx.container.running,
        connections: (await this.ctx.storage.get("connections")) ?? 0,
      });
    }
    return new Response("Not found", { status: 404 });
  }
}

export default {
  fetch(request, env) {
    // Public HTTP exposes status only. Startup is an internal ingress hook.
    if (request.method !== "GET" || new URL(request.url).pathname !== "/status") {
      return new Response("Not found", { status: 404 });
    }
    return env.ECHO.getByName("primary").fetch(request);
  },
};
