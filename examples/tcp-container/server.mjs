import net from "node:net";
import http from "node:http";

net.createServer({ allowHalfOpen: true }, (socket) => {
  console.log("TCP client connected");
  // A server-first protocol also proves ingress doesn't wait for a client
  // preamble to select the target.
  socket.write("READY\n");
  socket.on("data", (chunk) => {
    if (!socket.write(chunk)) socket.pause();
  });
  socket.on("drain", () => socket.resume());
  socket.on("end", () => socket.end("EOF\n"));
  socket.on("error", () => socket.destroy());
}).listen(7000, "0.0.0.0", () => {
  // Published Docker ports can accept before the process listens. Expose
  // application readiness only after the actual TCP server has bound.
  http.createServer((request, response) => {
    response.writeHead(request.url === "/ready" ? 204 : 404);
    response.end();
  }).listen(7001, "0.0.0.0");
});
