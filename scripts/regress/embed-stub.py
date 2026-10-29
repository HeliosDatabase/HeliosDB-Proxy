#!/usr/bin/env python3
"""Ollama-compatible embedding stub for the semantic-cache drills.

Usage: python3 -I scripts/regress/embed-stub.py [--port N] [--dim N] [--control FILE]

Listens on 127.0.0.1:${HELIOS_EMBED_STUB_PORT:-18434} (or --port). Standard library only.

  POST /api/embeddings  {"model", "prompt"} -> {"embedding": [...]}: a deterministic unit
                        vector of --dim (default 384) values per prompt; prompts of one
                        synonym group share a vector, so their cosine is 1.0
  GET  /api/tags        {"models": [{"name": "stub"}]}
  GET  /count           {"requests": N, "last_prompt": "...", "pid": P}: embedding requests
                        so far, and this process's id (a drill checks it answers itself)

The JSON control file, re-read on every request, steers the answers:
  {"mode": "ok" | "error" | "hang" | "wrongdim", "synonyms": [["val", "alt"]], "hang_secs": 10}
  ok        200 with the vector (the default, also when the file is missing)
  error     500
  hang      no answer for hang_secs (default 10), then the connection is closed
  wrongdim  200 with one value too many
"""
import argparse
import hashlib
import json
import math
import os
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE = {"requests": 0, "last_prompt": "", "pid": os.getpid()}
LOCK = threading.Lock()


def control(path):
    """The control file's settings (defaults when absent or unreadable)."""
    settings = {"mode": "ok", "synonyms": [], "hang_secs": 10}
    if path:
        try:
            with open(path, encoding="utf-8") as f:
                settings.update(json.load(f))
        except (OSError, ValueError):
            pass
    return settings


def vector(prompt, synonyms, dim):
    """A unit vector for `prompt`, shared by its synonym group."""
    key = "name:" + prompt
    for i, group in enumerate(synonyms):
        if prompt in group:
            key = f"group:{i}"
            break
    seed = int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    values = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(v * v for v in values)) or 1.0
    return [v / norm for v in values]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    dim = 384
    control_path = None

    def log_message(self, fmt, *args):  # quiet
        pass

    def reply(self, status, payload):
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def do_GET(self):
        if self.path == "/count":
            with LOCK:
                self.reply(200, dict(STATE))
        elif self.path == "/api/tags":
            self.reply(200, {"models": [{"name": "stub"}]})
        else:
            self.reply(404, {"error": "not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        try:
            request = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            request = {}
        if self.path != "/api/embeddings":
            self.reply(404, {"error": "not found"})
            return
        prompt = str(request.get("prompt", ""))
        with LOCK:
            STATE["requests"] += 1
            STATE["last_prompt"] = prompt
        settings = control(self.control_path)
        mode = settings.get("mode", "ok")
        if mode == "hang":
            time.sleep(float(settings.get("hang_secs", 10)))
            self.close_connection = True
            return
        if mode == "error":
            self.reply(500, {"error": "stub error mode"})
            return
        dim = self.dim + 1 if mode == "wrongdim" else self.dim
        self.reply(200, {"embedding": vector(prompt, settings.get("synonyms", []), dim)})


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--port", type=int, default=int(os.environ.get("HELIOS_EMBED_STUB_PORT", "18434"))
    )
    parser.add_argument("--dim", type=int, default=384)
    parser.add_argument("--control", help="JSON control file, re-read on every request")
    args = parser.parse_args()
    Handler.dim = args.dim
    Handler.control_path = args.control
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    server.daemon_threads = True
    print(f"embed-stub listening on 127.0.0.1:{args.port} dim={args.dim}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
