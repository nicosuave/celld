export default {
  async connect(socket, env, ctx) {
    await socket.readable.pipeTo(socket.writable);
  },
};
