# FUSE runtime regression

This fixture exercises the real celld config, generation, Docker create, and
container lifecycle paths. It needs a Linux Docker daemon (including a daemon
in a development VM) with registered `runsc` or `kata`. Kata also requires
working guest FUSE and `/dev/fuse` device provisioning. gVisor provides its
own in-sandbox device. The fixture uses fuse-overlayfs locally and needs
no bucket or credentials.

From the repository root:

```sh
mbx +stable build -p celld
uv run tests/container-fuse/check.py target/debug/celld runsc
# Or, on a daemon with Kata installed:
uv run tests/container-fuse/check.py target/debug/celld kata
```

The check starts an isolated `celld dev` on port 4987, verifies a FUSE read and
write followed by unmount, and verifies that the same image fails to mount in
a class without `fuse: true`. It inspects the actual Docker containers to check
the capability/device policy and checks the generated deployment feature gate.
It then repeats with the opt-in disabled, checking that mounting fails and the
deployment version changes while the FUSE feature requirement disappears.
The temporary project is removed and the node is stopped afterward. Images may
remain cached by Docker. Set `FUSE_TEST_PORT` to use another port.

The selected runtime applies to both the opted-in and restricted classes.
Default Docker/runc is intentionally unsupported. An earlier successful runc
smoke test does not qualify either sandbox runtime.
