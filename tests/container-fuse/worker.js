import { DurableObject } from "cloudflare:workers";

export class Mounted extends DurableObject {
  async fetch() {
    this.ctx.container.start({ enableInternet: true });
    try {
      await this.ctx.container.monitor();
      return new Response("FUSE mount succeeded\n");
    } catch (error) {
      return new Response(String(error), { status: 500 });
    }
  }
}

export class Restricted extends Mounted {}

export default {
  fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path === "/health") return new Response("ready");
    const namespace = path === "/mounted" ? env.MOUNTED : env.RESTRICTED;
    return namespace.getByName(crypto.randomUUID()).fetch(request);
  }
};
