# Capacity fallback egress tunnel

This optional systemd unit exposes a private SOCKS5 endpoint on the Docker
bridge. It gives only matching Antigravity capacity failures a second egress
route; normal requests remain direct.

The application and tunnel are configured independently:

- `ANTIGRAVITY_CAPACITY_FALLBACK_PROXY_URL` points the application at the
  private SOCKS endpoint, for example `socks5h://host.docker.internal:19080`.
- `/etc/gcli2api/capacity-egress-tunnel.env` selects the SSH target, port and
  account. Change `CAPACITY_EGRESS_SSH_HOST` to move the egress to another
  machine without changing application code.

Create a dedicated unprivileged account at both ends. Keep its private key and
`known_hosts` outside the repository with restrictive permissions. Bind the
SOCKS listener to the Docker bridge gateway only, and verify with `ss` that it
is not listening on a public interface.

The remote `authorized_keys` entry should use a dedicated key and restrict the
account to port forwarding. The tunnel does not accept or forward gcli2api API
requests itself, so it cannot call back into either application and cannot form
an application retry loop.
