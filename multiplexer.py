#!/usr/bin/env python3
"""
openai-compat-multiplexer
=========================

One endpoint in front of several OpenAI-compatible upstreams.

Why
---
A single-upstream setup fails loudly at the worst possible time. Multiplexing
lets you route around a dead upstream instead of returning 502s to your users.

What it does
------------
  * serves a single  POST /v1/chat/completions  and  GET /v1/models
  * routes by model name, trying upstreams in the configured order
  * marks an upstream unhealthy after N consecutive failures, retries it later
  * reports which upstream served the request in the  X-Upstream  header

Usage
-----
    python multiplexer.py --config config.json --port 8787

config.json
-----------
    {
      "upstreams": [
        {
          "name": "primary",
          "base_url": "https://upstream-a.example/v1",
          "api_key_env": "UPSTREAM_A_KEY",
          "models": ["gpt-4o-mini", "gpt-4o"]
        },
        {
          "name": "backup",
          "base_url": "https://upstream-b.example/v1",
          "api_key_env": "UPSTREAM_B_KEY",
          "models": ["*"]
        }
      ],
      "failure_threshold": 3,
      "cooldown_seconds": 60
    }

Deliberately dependency-free: the standard library only.
"""

import argparse
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATE_LOCK = threading.Lock()
STATE = {}  # name -> {"fails": int, "down_until": float}


class Upstream:
    def __init__(self, cfg):
        self.name = cfg["name"]
        self.base_url = cfg["base_url"].rstrip("/")
        self.api_key = os.environ.get(cfg.get("api_key_env", ""), "") or cfg.get("api_key", "")
        self.models = cfg.get("models") or ["*"]

    def serves(self, model):
        return "*" in self.models or model in self.models

    def available(self, cooldown):
        st = STATE.setdefault(self.name, {"fails": 0, "down_until": 0.0})
        return time.time() >= st["down_until"]

    def record(self, ok, threshold, cooldown):
        st = STATE.setdefault(self.name, {"fails": 0, "down_until": 0.0})
        with STATE_LOCK:
            if ok:
                st["fails"] = 0
                st["down_until"] = 0.0
            else:
                st["fails"] += 1
                if st["fails"] >= threshold:
                    st["down_until"] = time.time() + cooldown

    def status(self):
        st = STATE.setdefault(self.name, {"fails": 0, "down_until": 0.0})
        if time.time() < st["down_until"]:
            return "down (%ds left)" % int(st["down_until"] - time.time())
        return "up" if st["fails"] == 0 else "degraded (%d fails)" % st["fails"]


def forward(upstream, path, body, stream):
    """Send a request to one upstream. Returns (status, headers, payload|None, error)."""
    url = upstream.base_url + path
    req = urllib.request.Request(url, data=body, method="POST" if body else "GET")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "text/event-stream" if stream else "application/json")
    if upstream.api_key:
        req.add_header("Authorization", "Bearer " + upstream.api_key)
    req.add_header("User-Agent", "openai-compat-multiplexer")
    try:
        resp = urllib.request.urlopen(req, timeout=120)
        return resp.status, dict(resp.headers), resp, None
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), None, e.read().decode("utf-8", "replace")[:400]
    except Exception as e:
        return 0, {}, None, str(e)


def pick(upstreams, model, threshold, cooldown):
    """Upstreams that serve this model, healthy ones first."""
    candidates = [u for u in upstreams if u.serves(model)]
    healthy = [u for u in candidates if u.available(cooldown)]
    return healthy or candidates


class Handler(BaseHTTPRequestHandler):
    server_version = "openai-compat-multiplexer/0.1"
    config = None
    upstreams = []

    def log_message(self, fmt, *a):
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % a))

    def _send(self, status, payload, extra=None):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path.startswith("/v1/models"):
            ids = []
            for u in self.upstreams:
                for m in u.models:
                    if m != "*":
                        ids.append(m)
            self._send(200, {"object": "list",
                             "data": [{"id": i, "object": "model"} for i in sorted(set(ids))]})
            return
        if self.path.startswith("/health"):
            self._send(200, {"upstreams": [{"name": u.name, "status": u.status()}
                                           for u in self.upstreams]})
            return
        self._send(404, {"error": {"message": "not found"}})

    def do_POST(self):
        if not self.path.startswith("/v1/chat/completions"):
            self._send(404, {"error": {"message": "not found"}})
            return

        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            self._send(400, {"error": {"message": "invalid JSON body"}})
            return

        model = body.get("model") or ""
        stream = bool(body.get("stream"))
        threshold = self.config["failure_threshold"]
        cooldown = self.config["cooldown_seconds"]

        tried = []
        for up in pick(self.upstreams, model, threshold, cooldown):
            status, headers, resp, err = forward(up, "/chat/completions", raw, stream)
            if status == 200 and resp is not None:
                up.record(True, threshold, cooldown)
                self.send_response(200)
                self.send_header("Content-Type",
                                 headers.get("Content-Type", "application/json"))
                self.send_header("X-Upstream", up.name)
                if stream:
                    self.send_header("Cache-Control", "no-cache")
                    self.end_headers()
                    try:
                        while True:
                            chunk = resp.read(1024)
                            if not chunk:
                                break
                            self.wfile.write(chunk)
                            self.wfile.flush()
                    except Exception:
                        pass
                else:
                    payload = resp.read()
                    self.send_header("Content-Length", str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)
                return

            up.record(False, threshold, cooldown)
            tried.append("%s -> %s %s" % (up.name, status, (err or "")[:80]))

        self._send(502, {"error": {"message": "all upstreams failed for model '%s'" % model,
                                   "tried": tried}})


def main():
    ap = argparse.ArgumentParser(description="Multiplex OpenAI-compatible upstreams.")
    ap.add_argument("--config", required=True)
    ap.add_argument("--port", type=int, default=8787)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    with open(args.config, encoding="utf-8") as fh:
        cfg = json.load(fh)

    cfg.setdefault("failure_threshold", 3)
    cfg.setdefault("cooldown_seconds", 60)

    upstreams = [Upstream(u) for u in cfg["upstreams"]]
    if not upstreams:
        print("config has no upstreams", file=sys.stderr)
        return 2

    Handler.config = cfg
    Handler.upstreams = upstreams

    print("listening on http://%s:%d" % (args.host, args.port))
    for u in upstreams:
        print("  upstream %-12s %s  models=%s  key=%s"
              % (u.name, u.base_url, ",".join(u.models),
                 "set" if u.api_key else "MISSING"))
    ThreadingHTTPServer((args.host, args.port), Handler).serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
