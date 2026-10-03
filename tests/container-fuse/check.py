# /// script
# requires-python = ">=3.11"
# dependencies = []
# ///
"""Run the credential-free FUSE regression against a built celld binary."""

import json
import os
from pathlib import Path
import shutil
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True, stderr=subprocess.STDOUT)


def request(base, path):
    try:
        with urllib.request.urlopen(base + path, timeout=60) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()


def check(binary, enabled, runtime):
    fixture = Path(__file__).resolve().parent
    port = os.environ.get("FUSE_TEST_PORT", "4987")
    base = f"http://127.0.0.1:{port}"
    with tempfile.TemporaryDirectory(prefix="celld-fuse-") as temporary:
        project = Path(temporary)
        for name in ["Dockerfile", "mount.sh", "wrangler.jsonc", "worker.js"]:
            shutil.copyfile(fixture / name, project / name)
        config_path = project / "wrangler.jsonc"
        config = json.loads(config_path.read_text())
        config["containers"][0]["fuse"] = enabled
        for declaration in config["containers"]:
            declaration["runtime"] = runtime
        config_path.write_text(json.dumps(config))
        log_path = project / "node.log"
        with log_path.open("w") as log:
            process = subprocess.Popen(
                [str(binary), "dev", str(project), "--port", port, "--no-watch", "--logs"],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
            )
            try:
                deadline = time.monotonic() + 180
                while True:
                    if process.poll() is not None:
                        raise AssertionError("celld exited before readiness")
                    try:
                        if request(base, "/health") == (200, "ready"):
                            break
                    except (OSError, urllib.error.URLError):
                        pass
                    if time.monotonic() >= deadline:
                        raise AssertionError("celld did not become ready")
                    time.sleep(0.2)

                databases = list((project / ".celld").rglob("objects.sqlite3"))
                assert len(databases) == 1, databases
                with sqlite3.connect(databases[0]) as connection:
                    rows = connection.execute("select body from objects where key like '%/manifest.json'").fetchall()
                manifests = [json.loads(body) for (body,) in rows]
                assert len(manifests) == 1, manifests
                manifest = manifests[0]
                assert ("containers-fuse-v2" in manifest["required_features"]) == enabled, manifest
                specs = {spec["class_name"]: spec for spec in manifest["containers"]}
                assert specs["Mounted"].get("fuse", False) == enabled
                assert not specs["Restricted"].get("fuse", False)

                for path, expected_status, expected_fuse in [
                    ("/mounted", 200 if enabled else 500, enabled), ("/restricted", 500, False)
                ]:
                    status, body = request(base, path)
                    assert status == expected_status, (path, status, body)
                    class_name = "Mounted" if path == "/mounted" else "Restricted"
                    # Image identity ties inspection to this deployment, even
                    # if another developer is using the same class names.
                    ids = docker("ps", "-aq", "--filter", f"label=celld.class={class_name}",
                                 "--filter", f"ancestor={specs[class_name]['image']}").split()
                    assert len(ids) == 1, ids
                    container = json.loads(docker("inspect", ids[0]))[0]
                    host = container["HostConfig"]
                    assert not host["Privileged"]
                    assert host["CapDrop"] == ["ALL"]
                    assert "no-new-privileges" in host["SecurityOpt"]
                    assert host["CapAdd"] == (["SYS_ADMIN"] if expected_fuse else None)
                    assert host["Runtime"] == runtime
                    assert bool(host["Devices"]) == (expected_fuse and runtime == "kata")
                    output = docker("logs", ids[0])
                    if expected_fuse:
                        assert "FUSE read, write, and unmount passed" in output, output
                    else:
                        assert container["State"]["ExitCode"] != 0, container["State"]
                    print(f"{class_name}: HTTP {status}; FUSE {'succeeded' if expected_fuse else 'denied'}")
                print("Deployment feature gate and container policy passed")
                return manifest["version"]
            except BaseException:
                print(log_path.read_text(), file=sys.stderr)
                raise
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGINT)
                    try:
                        process.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGTERM)
                        process.wait(timeout=10)


if __name__ == "__main__":
    binary = Path(sys.argv[1]).resolve()
    runtime = sys.argv[2]
    if runtime not in {"runsc", "kata"}:
        raise SystemExit("runtime must be runsc or kata")
    enabled_version = check(binary, True, runtime)
    disabled_version = check(binary, False, runtime)
    assert enabled_version != disabled_version, (enabled_version, disabled_version)
    print("Disabling FUSE changes deployment identity and removes the feature gate")
