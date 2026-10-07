"""
Run the Stock Oracle Terminal.

    python -m stock_oracle.terminal                 # this PC only: http://127.0.0.1:8765
    python -m stock_oracle.terminal --lan           # also reachable over Tailscale / LAN
    python -m stock_oracle.terminal --port 9000 --no-browser

The terminal always requires a token (TERMINAL_TOKEN in .env, or a random one
created on first run and kept in stock_oracle/data/terminal_token.txt). Open
the printed URL once per browser; a cookie remembers it after that.
"""
import argparse
import ipaddress
import logging
import os
import socket
import sys
import threading
import webbrowser


def _local_ips():
    ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None, socket.AF_INET):
            ips.add(info[4][0])
    except Exception:
        pass
    return sorted(ip for ip in ips if not ip.startswith("127."))


def main():
    parser = argparse.ArgumentParser(prog="python -m stock_oracle.terminal",
                                     description="Stock Oracle Terminal — Bloomberg-style web terminal")
    parser.add_argument("--host", default=None, help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=None, help="port (default 8765)")
    parser.add_argument("--lan", action="store_true",
                        help="listen on all interfaces so phones/laptops on Tailscale or LAN can connect")
    parser.add_argument("--token", default=None,
                        help="access token (default: TERMINAL_TOKEN, else data/terminal_token.txt)")
    parser.add_argument("--no-browser", action="store_true", help="don't open a browser tab")
    args = parser.parse_args()

    # Several Oracle modules use paths relative to the repo root
    # (stock_oracle/data/...), exactly like START.bat does.
    from stock_oracle.terminal import settings, __version__
    os.chdir(settings.REPO_DIR)

    host = args.host or ("0.0.0.0" if args.lan else settings.get("TERMINAL_HOST", settings.DEFAULT_HOST))
    port = args.port or settings.get_int("TERMINAL_PORT", settings.DEFAULT_PORT)
    try:
        loopback = ipaddress.ip_address(host).is_loopback
    except ValueError:
        loopback = host == "localhost"

    token = args.token or settings.access_token()

    logging.getLogger("stock_oracle.terminal").setLevel(logging.INFO)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    from stock_oracle.terminal.server import create_app
    # LAN/Tailscale names vary too much to list, so the Host check is loopback-only;
    # off loopback the token cookie (bound to the host name) does that job.
    app = create_app(token=token, trusted_hosts=settings.LOOPBACK_HOSTS if loopback else None)

    suffix = f"/?token={token}"
    local_url = f"http://127.0.0.1:{port}{suffix}"
    keys = settings.key_status()
    print()
    print(f"  STOCK ORACLE TERMINAL v{__version__}")
    print("  " + "-" * 45)
    print(f"  This PC:   {local_url}")
    if not loopback:
        for ip in _local_ips():
            print(f"  Network:   http://{ip}:{port}{suffix}")
        print(f"  Hostname:  http://{socket.gethostname()}:{port}{suffix}")
    print(f"  Keys:      Finnhub {'yes' if keys['finnhub'] else 'no '} | "
          f"Alpaca {'yes' if keys['alpaca'] else 'no '} | Anthropic {'yes' if keys['anthropic'] else 'no'}")
    if not keys["finnhub"] and not keys["alpaca"]:
        print("             (no streaming key: quotes refresh from Yahoo every "
              f"{settings.poll_seconds()}s — add a free Finnhub key in Settings for live ticks)")
    print("  Stop:      Ctrl+C")
    print()

    if not args.no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(local_url)).start()

    import uvicorn
    uvicorn.run(app, host=host, port=port, log_level="warning", ws_ping_interval=20, ws_ping_timeout=20)


if __name__ == "__main__":
    sys.exit(main())
