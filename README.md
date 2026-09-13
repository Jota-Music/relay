# Jota Relay

Optional WebSocket relay for [Jota](https://github.com/Jota-Music/jota) listening sessions.

Jota syncs playback between devices without a central account. Each session is a room
identified by a short code that the host shares. The relay only forwards messages
between the members of a room and caches the last playback state so late joiners and
reconnects catch up instantly.

## Run

```sh
go build ./... && ./relay
# or
docker run -e PORT=8080 -p 8080:8080 ghcr.io/jota-music/relay
```

Configuration (environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP listen port. |
| `MAX_GUESTS` | `8` | Max guests per room (host not counted). |
| `ROOM_TTL` | `10m` | How long a room survives without a host before it is closed. |
| `AUTH_TOKEN` | _(empty)_ | Shared secret required on `/ws`. Empty disables auth (relay is open). |

When `AUTH_TOKEN` is set, clients must send it as `Authorization: Bearer <token>`
(or `?token=<token>`). Requests without a valid token get `401` before the WebSocket
upgrade. `/healthz` stays open. The Jota app exposes a "Token" field and embeds it in
the shared invite, so guests never type it.

Rooms can additionally be protected with an optional **password**. The first member to
join sets it; later joins must present the same value via `X-Room-Password` (or
`?pass=`) or they get an `error` and the connection closes. An empty password leaves
the room open. The app keeps the room password out of the invite so the code alone is
not enough.

Put it behind TLS (Caddy/nginx) so clients can use `wss://`. The app accepts either
`wss://relay.example.com` or `https://relay.example.com` (the `/ws` path is added
automatically), and `ws://`/`http://` for local testing.

## Protocol

Connect to `/ws?room=CODE&role=host|guest`. `CODE` is a shared secret (1–64 chars,
`[A-Za-z0-9_-]`). The first `host` claims the room; a second one is rejected with an
`error`. Guests are capped by `MAX_GUESTS`. Omit `role` (or send it empty) to let the
relay decide: it grants `host` when the room has none and `guest` otherwise, then sends
the assigned role back as a `role` message.

Messages are JSON text frames with a `t` field. The relay is a pipe: it forwards every
frame to the other room members and understands only these control types.

Client → relay:

| Message | Description |
|---------|-------------|
| `{"t":"ping","id":N,"at":MS}` | Clock sync. Answered locally; never forwarded. |
| `{"t":"state",...}` | Host playback state. Cached as the room snapshot. |
| `{"t":"queue",...}` | Host queue. Cached as the room snapshot. |
| `{"t":"heartbeat",...}` | Host heartbeat. Forwarded. |

Relay → client:

| Message | Description |
|---------|-------------|
| `{"t":"pong","id":N,"at":MS,"echo":SERVER_MS}` | Reply to `ping`. `echo` is the server clock. |
| `{"t":"role","role":"host\|guest"}` | Assigned role. Only sent when connecting without an explicit `role`. |
| `{"t":"members","count":N}` | Sent to everyone on connect and on every membership change. |
| `{"t":"error","reason":"..."}` | Fatal room error; the connection is then closed. |

On connect the relay sends `members`, then replays the cached `queue` (if any) followed
by the cached `state` (if any). Queue is replayed first so the state's track index
resolves.

The `state`/`queue`/`heartbeat` shapes live in the app, not here — see
`frontend/src/lib/sync/model` in the Jota repo. Both sides must keep their JSON in
sync (same convention as the Spotify models).
