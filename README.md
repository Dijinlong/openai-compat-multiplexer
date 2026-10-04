# openai-compat-multiplexer

One endpoint in front of several OpenAI-compatible upstreams.

## Why

Single-upstream setups fail loudly at the worst possible time. Multiplexing lets you
route around a dead upstream instead of returning 502s to your users.

## What it does

- Exposes a single `/v1/chat/completions` (and `/v1/models`)
- Routes by model name, with a configurable upstream priority
- Marks an upstream unhealthy after N consecutive failures, and retries it later
- Reports which upstream actually served a request

## Minimal config

    upstreams:
      - name: primary
        base_url: https://upstream-a.example/v1
        api_key_env: UPSTREAM_A_KEY
        models: ["gpt-4o-mini", "gpt-4o"]
      - name: backup
        base_url: https://upstream-b.example/v1
        api_key_env: UPSTREAM_B_KEY
        models: ["*"]

## Status

Early. The routing core works; observability is thin.
