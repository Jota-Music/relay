# Jota Relay

Optional WebSocket relay for [Jota](https://github.com/Jota-Music/jota) listening
sessions, developed by [salvadorsru](https://github.com/salvadorsru).

Jota syncs playback between devices without a central account. Each session is a
room identified by a short code the host shares. The relay forwards messages
between room members and caches the last playback state, so late joiners and
reconnects catch up instantly.

[![License: GPL-3.0](https://img.shields.io/badge/license-GPL--3.0-4ea94b.svg)](LICENSE)

## Run

```sh
go build ./... && ./relay
# or
docker run -e PORT=8080 -p 8080:8080 ghcr.io/jota-music/relay
```

## Configure

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP listen port. |
| `MAX_GUESTS` | `8` | Max guests per room (host not counted). |
| `ROOM_TTL` | `10m` | Room lifespan without a host before it closes. |
| `AUTH_TOKEN` | _(empty)_ | Shared secret for `/ws`; empty keeps the relay open. |

**Auth.** With `AUTH_TOKEN` set, clients send `Authorization: Bearer <token>`
(or `?token=<token>`); requests without it get `401` before the upgrade.
`/healthz` stays public and returns `{"auth":true|false}` so clients can tell
whether a token is required.

**Room password.** The first member sets it; later joins must send it via
`X-Room-Password` (or `?pass=`) or the connection closes. An empty password
leaves the room open.

Host it behind TLS (Caddy/nginx) and point the app at `wss://` or `https://`
(the `/ws` path is added automatically); `ws://`/`http://` works for local
testing.

## Protocol

`/ws?room=CODE&role=host|guest`. `CODE` is a shared secret (1–64 chars,
`[A-Za-z0-9_-]`). The first host claims the room (a second one is rejected);
guests are capped by `MAX_GUESTS`. Omit `role` to let the relay decide — it
replies with the assigned role.

Messages are JSON text frames with a `t` field. The relay forwards every frame
to the other members and understands only these control types:

| Direction | Message | Description |
|-----------|---------|-------------|
| Client → relay | `{"t":"ping","id":N,"at":MS}` | Clock sync; answered locally, never forwarded. |
| Client → relay | `{"t":"state",...}` | Host playback state; cached as the room snapshot. |
| Client → relay | `{"t":"queue",...}` | Host queue; cached as the room snapshot. |
| Client → relay | `{"t":"heartbeat",...}` | Host heartbeat; forwarded. |
| Relay → client | `{"t":"pong","id":N,"at":MS,"echo":SERVER_MS}` | Reply to `ping`; `echo` is the server clock. |
| Relay → client | `{"t":"role","role":"host\|guest"}` | Assigned role; only when connecting without one. |
| Relay → client | `{"t":"members","count":N}` | Sent on connect and every membership change. |
| Relay → client | `{"t":"error","reason":"..."}` | Fatal room error; sets the connection to close. |

On connect the relay sends `members`, then replays the cached `queue` followed
by the cached `state` (queue first so track indexes resolve).

The `state`/`queue`/`heartbeat` shapes live in the Jota app, not here — see
`frontend/src/lib/sync/model` in the Jota repo. Both sides must keep their JSON
in sync.

## License

[GPL-3.0](LICENSE) · Copyright (C) 2026 salvadorsru
