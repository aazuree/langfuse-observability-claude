# Remote / LAN Deployment (run Langfuse on your Pi-hole)

By default this stack runs entirely on one machine and binds every port to
`127.0.0.1` — nothing is reachable from the network. This guide adds an **opt-in
LAN mode**: run the containers on another host (e.g. the box running Pi-hole) and
expose *only* the dashboard to local-network devices, while your dev machine's
Claude Code hook ships traces to it over the LAN.

Both modes come from the same repo — you choose by setting a few `.env` values.
Nothing here changes the default local experience.

| | Local mode (default) | LAN / Pi mode |
|---|---|---|
| Where containers run | your dev machine | the Pi-hole host |
| `LANGFUSE_WEB_BIND` | `127.0.0.1` | the host's LAN IP |
| `NEXTAUTH_URL` | `http://localhost:3100` | `http://<pi-lan-ip>:3100` |
| Hook `LANGFUSE_HOST` | unset (defaults to localhost) | `http://<pi-lan-ip>:3100` |
| Dashboard reachable from | this machine only | any device on your LAN |

---

## What actually gets exposed

Only **`langfuse-web` (port 3100)** is published beyond localhost. It serves both
the dashboard UI *and* the `/api/public/ingestion` endpoint the hook POSTs to, so a
single interface change covers both. PostgreSQL, ClickHouse, Redis, MinIO and the
worker stay bound to `127.0.0.1` on the Pi — no reason to put a database on the LAN.

"LAN only" here = bind the published port to the host's **LAN IP** instead of
`0.0.0.0`. On a single-NIC Pi behind a home router this means the port listens on
the LAN interface and is never reachable from the internet **as long as your router
does not forward port 3100**. Do not add a port-forward for it. There is no TLS —
this is plain HTTP intended for a trusted home LAN only.

---

## Before you start (Pi-hole host requirements)

- **64-bit OS (arm64) or x86.** ClickHouse has **no 32-bit / armv7 image** — a
  32-bit Pi OS will not run this stack. Check with `uname -m` → expect `aarch64`
  or `x86_64`.
- **≥ 4 GB RAM.** This is a 6-container stack; ClickHouse is the memory hog. On a
  4 GB Pi, add swap (e.g. 2 GB zram or a swapfile) to survive spikes. A 1–2 GB
  board will OOM.
- **Docker + Docker Compose plugin** installed on the Pi.
- The Pi's **LAN IP** (`hostname -I | awk '{print $1}'`), ideally a DHCP
  reservation so it doesn't move.

---

## Step 1 — Bring up the stack on the Pi

On the **Pi-hole host**:

```bash
git clone https://github.com/aazuree/langfuse-observability.git
cd langfuse-observability

# Generate a LAN-ready .env in one shot (replace with your Pi's LAN IP).
# setup.sh reads these two env vars; everything else is auto-generated.
PI_IP=192.168.1.50
NEXTAUTH_URL=http://$PI_IP:3100 LANGFUSE_WEB_BIND=$PI_IP ./setup.sh
```

`setup.sh` also tries to configure a Claude Code Stop hook in `~/.claude/settings.json`
on whatever machine it runs on. On a headless Pi that has no Claude Code, that hook
is harmless (it just writes a settings file that never fires) — ignore it. The hook
that matters lives on your **dev machine** (Step 2).

> Prefer not to run `setup.sh` on the Pi at all? Copy `.env.example` to `.env`,
> fill in the `CHANGEME` secrets, set `NEXTAUTH_URL=http://<pi-ip>:3100` and
> `LANGFUSE_WEB_BIND=<pi-ip>`, then `docker compose up -d`.

Wait for the containers, then from **another LAN device** open
`http://<pi-ip>:3100` and finish first-login (incognito if you hit a CSRF error).

**Grab the project keys** — you'll need them on the dev machine:

```bash
grep '^LANGFUSE_INIT_PROJECT_PUBLIC_KEY=' .env   # pk-lf-...
grep '^LANGFUSE_INIT_PROJECT_SECRET_KEY=' .env   # sk-lf-...
```

## Step 2 — Point your dev machine's hook at the Pi

On your **dev machine** (where Claude Code runs), edit `~/.claude/settings.json`.
Add `LANGFUSE_HOST=http://<pi-ip>:3100` to the front of the hook commands, and use
the **pk/sk from the Pi's `.env`** (Step 1) — the keys must match the Pi's project:

```json
{
  "hooks": {
    "Stop": [
      { "hooks": [ { "type": "command",
        "command": "LANGFUSE_HOST=http://192.168.1.50:3100 LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-... python3 /path/to/langfuse-hook.py"
      } ] }
    ],
    "StopFailure": [
      { "hooks": [ { "type": "command",
        "command": "LANGFUSE_HOST=http://192.168.1.50:3100 LANGFUSE_PUBLIC_KEY=pk-lf-... LANGFUSE_SECRET_KEY=sk-lf-... python3 /path/to/session-start-hook.py"
      } ] }
    ]
  }
}
```

The dev machine keeps its **local clone of this repo** for the hook scripts; only
the containers moved to the Pi.

## Step 3 — Verify end-to-end

```bash
# From the dev machine: the Pi's ingestion API is reachable
curl -sf http://<pi-ip>:3100 >/dev/null && echo "reachable"

# Run any Claude Code turn, then tail the hook log
tail -f ~/.claude/langfuse-hook.log
```

A new trace should appear in the dashboard at `http://<pi-ip>:3100` within ~10s
(ClickHouse ingestion is async). Confirm the dashboard also loads from a *second*
LAN device (phone/laptop).

---

## Firewall gotcha (read this)

**Docker's published ports bypass the host `INPUT` firewall.** Docker inserts its
own iptables rules (the `DOCKER`/`DOCKER-USER` chains) that are evaluated *before*
`ufw`/`nftables` INPUT rules — so a plain `ufw deny 3100` will **not** block a
published port. Two things actually work:

1. **Binding to the LAN IP (what this guide does).** With
   `LANGFUSE_WEB_BIND=<lan-ip>`, the port only listens on that interface. Verify:

   ```bash
   ss -tlnp | grep :3100     # should show <lan-ip>:3100, NOT 0.0.0.0:3100
   ```

   Combined with no router port-forward, this keeps it off the internet.

2. **Strict source-IP allowlist (optional, defense-in-depth).** If the Pi is
   multi-homed or you want to restrict to your LAN subnet even on that interface,
   add a `DOCKER-USER` rule (survives container restarts, unlike INPUT rules):

   ```bash
   # Allow only 192.168.1.0/24 to reach published container ports; drop the rest.
   sudo iptables -I DOCKER-USER -i <wan-iface> ! -s 192.168.1.0/24 -j DROP
   ```

   Persist it with your distro's iptables-save mechanism. Skip this if the Pi has
   one NIC and no port-forward — binding to the LAN IP is already sufficient there.

---

## Reaching it from outside your LAN (WireGuard — no public HTTP)

LAN mode above exposes the dashboard to your *local* network. To reach it while
**away from home** — so the Claude Code hook keeps logging on the road — do **not**
port-forward `3100` to the internet. It's plain HTTP: your project secret key would
travel in cleartext on every ingestion POST, the login UI would be brute-forceable,
and Docker's published ports bypass the host firewall anyway (see the gotcha above).

Instead, put a **WireGuard** tunnel in front and expose only WireGuard's one UDP
port. The dashboard and ingestion endpoint stay reachable **only inside the
encrypted tunnel**; the single thing facing the internet is a UDP port that answers
nothing without a valid peer key — invisible to scanners.

**The model — no rebind, hook address unchanged:**

- Keep `langfuse-web` bound to the Pi's LAN IP (LAN mode, as above). Do **not**
  rebind to `0.0.0.0`.
- Add each roaming device as a WireGuard **peer**, and put the Pi's LAN IP in that
  peer's `AllowedIPs`. Tunnelled traffic to `<pi-lan-ip>:3100` then reaches the Pi,
  where Docker's DNAT delivers it to the container.
- The hook keeps targeting `http://<pi-lan-ip>:3100` — direct when you're home,
  transparently tunnel-routed when you're away. **No `settings.json` change.**

**Steps** (assume a WireGuard server already on the Pi, `wg0 = <wg-subnet>`, e.g.
`10.0.0.1/24`):

1. **Add the roaming device as a peer on the Pi** (live + persisted so it survives
   reboot):

   ```bash
   # on the Pi — <device-pubkey> from `wg genkey | wg pubkey` on the device
   sudo wg set wg0 peer <device-pubkey> allowed-ips <wg-device-ip>/32
   printf '\n[Peer]\nPublicKey = <device-pubkey>\nAllowedIPs = <wg-device-ip>/32\n' \
     | sudo tee -a /etc/wireguard/wg0.conf >/dev/null
   ```

2. **Roaming-device config** (`/etc/wireguard/wg-<name>.conf`, `chmod 600` — never
   commit or print the private key):

   ```ini
   [Interface]
   PrivateKey = <device-privkey>
   Address    = <wg-device-ip>/32

   [Peer]
   PublicKey           = <pi-wg-pubkey>
   Endpoint            = <pi-public-ip-or-ddns>:51820
   AllowedIPs          = <wg-subnet>, <pi-lan-ip>/32
   PersistentKeepalive = 25
   ```

3. **Router: forward UDP 51820 → the Pi — that one port only.**

   > ⚠️ **If the Pi also runs Pi-hole** (or any DNS / other UDP service), do **not**
   > use an "Any(UDP)" / all-ports rule. That exposes the resolver on **UDP 53** to
   > the internet = an **open DNS resolver** (amplification abuse, IP blacklisting).
   > Restrict the forward to the single port `51820`. Pi-hole's `listeningMode` is
   > *not* a substitute — `SINGLE`/interface modes don't filter by source; only the
   > port restriction (or a source-filtering `LOCAL` mode) keeps 53 safe.

4. **Bring the tunnel up and verify:**

   ```bash
   sudo wg-quick up wg-<name>
   sudo wg show wg-<name>          # want 'latest handshake' + rx/tx > 0
   curl -s -o /dev/null -w '%{http_code}\n' http://<pi-lan-ip>:3100   # want 200
   ```

   Run any Claude Code turn; the trace lands on the Pi.

**Notes:**

- **Only UDP 51820 is exposed.** WireGuard (Curve25519 / ChaCha20-Poly1305) silently
  drops any packet not signed by a known peer key — no banner, no reflection vector.
  The HTTP inside is wrapped in the tunnel's encryption on the wire.
- **Dynamic public IP?** Point `Endpoint` at a DDNS hostname tracking the Pi's WAN
  IP, or just re-edit the one line when it rotates.
- **Always-on vs travel-only.** If your router does **NAT hairpin** (the tunnel
  handshakes even while you're on the home LAN), `systemctl enable --now
  wg-quick@wg-<name>` and never toggle it. Otherwise run it as a travel tool
  (`wg-quick up` when away, `down` at home) — which also keeps the home hook fully
  local, with no dependency on your internet uplink.
- **Blast radius.** A peer's private key grants tunnel access; if the Pi has
  `ip_forward=1` it can reach the whole LAN, not just the container. Use full-disk
  encryption on roaming devices, keep key files `chmod 600`, and optionally add an
  `iptables -I FORWARD` rule limiting `wg0`→LAN to just the Pi's IP.

Reference: [wireguard.com](https://www.wireguard.com/) ·
[Pi-hole docs](https://docs.pi-hole.net/).

---

## Switching back to local mode

On whichever machine you want local-only again:

```bash
# In .env:
NEXTAUTH_URL=http://localhost:3100
LANGFUSE_WEB_BIND=127.0.0.1
docker compose up -d      # re-publishes on 127.0.0.1
```

On the dev machine, drop the `LANGFUSE_HOST=...` prefix from the hook commands so
it defaults back to `http://localhost:3100`.

---

## Langfuse v4 (running since 2026-08-26)

The stack runs **Langfuse v4** with `LANGFUSE_MIGRATION_V4_WRITE_MODE=legacy`.

v4 moves to an observations-first data model (`events_full` / `events_core`)
that replaces the v3 `traces` / `observations` tables, but the migration is
staged and reversible: in `legacy` mode v4 keeps writing the v3 tables and
keeps the legacy batch ingestion endpoints serving. Historic traces stay
browsable as virtual root spans, and the ClickHouse schema migrations apply
automatically on first start.

> ⚠️ **Do not set `events_only`.** That is the v4 cutover, and it replaces the
> legacy batch ingestion endpoints with OpenTelemetry-based ingestion.
> `langfuse-hook.py` POSTs to `/api/public/ingestion` over plain stdlib HTTP,
> so the cutover breaks ingestion outright until the hook is ported to OTel.
> `dual` is safe (it writes both), but adds roughly a 15-minute UI delay for
> non-SDK producers like ours, which cannot propagate attributes client-side.

Infrastructure already satisfies the v4 floors — ClickHouse 25.12 minimum
(26.4 recommended), PostgreSQL 15 minimum, Redis 7.0 minimum. Back up both
PostgreSQL **and** ClickHouse before any further migration step.

## Known limitations

- **No TLS.** Traffic (including the project secret key on ingestion POSTs) is
  plain HTTP on the LAN. Fine for a trusted home network; do not route it over the
  internet. Front it with a reverse proxy (Caddy/nginx + a LAN cert) if you want
  HTTPS.
- **MinIO media links break cross-host.** `LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT`
  defaults to `http://localhost:9190`, so presigned media URLs point at *localhost*
  and won't resolve from another device. Claude Code traces are text-only, so this
  doesn't affect this hook — but if you ever store media, point that endpoint at
  the Pi's LAN IP and publish MinIO (`9190`) too.
- **Keys must match.** The dev-machine hook's pk/sk must equal the Pi's
  `LANGFUSE_INIT_PROJECT_*` keys, or ingestion 401s (silently, into the hook log).
- **Clock skew.** Cost/latency math uses turn timestamps from the dev machine's
  transcript; the Pi only stores them. No action needed, just be aware trace times
  reflect the dev machine's clock.
