"""``ifetch serve`` - the same commands, in a browser, on this machine only.

Starts a small HTTP server on 127.0.0.1 and prints a URL containing a
single-use-looking random token. Opening it sets an HttpOnly cookie and the
token leaves the address bar. Nothing is written to disk and the token dies
with the process, so stopping the server revokes access.

Two flags exist because someone will want them and both deserve a warning
rather than a footnote:

``--host``
    Binds somewhere other than loopback. This process holds an authenticated
    iCloud session, so that exposes a person's file listings, their Apple ID
    email, and the ability to start downloads onto this disk to everything that
    can reach the address. The warning printed at startup says so by name.

``--allow-path``
    Adds a folder that downloads may be written to. Without it the destination
    has to be inside the home directory or a mounted volume, because a path
    arriving from a browser is not a path a person necessarily typed.

The Apple ID password is never part of this. It is read here, from the system
keyring or ``--password-command``, exactly as the download command reads it,
and handed straight to the downloader.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any, List, Optional, Sequence

from .webui.server import (
    LOOPBACK_HOSTS,
    WILDCARD_HOSTS,
    build_manager_factory,
    build_password_provider,
    create_server,
    default_allowed_roots,
)

EXIT_OK = 0
EXIT_ERROR = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ifetch serve",
        description=(
            "Run the iFetch web UI on this machine. Binds 127.0.0.1 and prints "
            "a URL containing the access token."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1",
                        help="Interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8765,
                        help="Port to bind, 0 for any free one (default: 8765)")
    parser.add_argument("--email", help="Pre-fill the Apple ID email (or set $ICLOUD_EMAIL)")
    parser.add_argument("--region", choices=["global", "china"], default=None)
    parser.add_argument("--password-command", dest="password_command",
                        help="Shell-free command that prints the Apple ID password on stdout")
    parser.add_argument("--max-workers", type=int, default=4,
                        help="Concurrent downloads per run (default: 4)")
    parser.add_argument("--local-path", dest="local_path", default=None,
                        help="Destination the page offers by default "
                             "(default: ~/icloud-backup)")
    parser.add_argument("--allow-path", dest="allow_paths", action="append", default=[],
                        metavar="DIR",
                        help="Also permit downloads into this folder. Repeatable.")
    parser.add_argument("--allow-host", dest="allow_hosts", action="append", default=[],
                        metavar="NAME",
                        help="Also accept this Host header. Needed only when reaching "
                             "the UI by a name other than localhost. Repeatable.")
    parser.add_argument("--open", dest="open_browser", action="store_true",
                        help="Open the URL in the default browser")
    return parser


def _warn_about_host(host: str, port: int, allow_hosts: Sequence[str], stdout: Any) -> None:
    """Say exactly what a non-loopback bind has exposed, and to whom."""
    if host in LOOPBACK_HOSTS:
        return
    who = (
        "Anyone who can reach this machine"
        if host in WILDCARD_HOSTS
        else f"Anyone who can reach {host}"
    )
    print(
        f"WARNING: binding to {host}:{port} exposes this server beyond this machine.\n"
        "  It holds an authenticated iCloud session. "
        f"{who} and has the\n"
        "  token can list your iCloud Drive, read your Apple ID email, and start\n"
        "  downloads onto this disk. The connection is plain HTTP, so the token\n"
        "  crosses the network in the clear. Prefer 127.0.0.1 with an SSH tunnel\n"
        "  unless you are certain.",
        file=stdout,
    )
    if host in WILDCARD_HOSTS and not allow_hosts:
        print(
            "  Note: requests are still refused unless their Host header is a "
            "loopback name.\n"
            "  Pass --allow-host <name> for the name you will actually type.",
            file=stdout,
        )


def _configure_logging() -> None:
    """Show the access log, scoped to this package.

    Without a handler a server that is refusing every request looks exactly
    like a server that is not running. The root logger is left alone so that
    embedding ``ifetch serve`` does not reconfigure someone else's logging.
    """
    logger = logging.getLogger("ifetch.webui")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def run(args: argparse.Namespace, stdout: Any) -> int:
    roots: List[Path] = default_allowed_roots()
    roots.extend(Path(p).expanduser() for p in args.allow_paths)

    server = create_server(
        host=args.host,
        port=args.port,
        allow_hosts=args.allow_hosts,
        manager_factory=build_manager_factory(
            region=args.region, max_workers=args.max_workers
        ),
        password_provider=build_password_provider(args.password_command),
        default_email=args.email,
        allowed_roots=roots,
        default_local=Path(args.local_path).expanduser() if args.local_path else None,
    )

    _configure_logging()
    _warn_about_host(args.host, server.port, args.allow_hosts, stdout)
    print(f"iFetch web UI: {server.url}", file=stdout)
    print("The token is in that URL and nowhere else. Ctrl-C to stop.", file=stdout)

    if args.open_browser:
        import webbrowser

        webbrowser.open(server.url)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping.", file=stdout)
    finally:
        server.stop()
    return EXIT_OK


def main(argv: Optional[Sequence[str]] = None, stdout: Any = None) -> int:
    stream = stdout if stdout is not None else sys.stdout
    args = build_parser().parse_args(list(argv) if argv is not None else None)
    try:
        return run(args, stream)
    except OSError as exc:
        print(f"Error: could not start the server: {exc}", file=sys.stderr)
        return EXIT_ERROR
    except Exception as exc:  # noqa: BLE001 - the reason belongs on stderr
        print(f"Error: {exc}", file=sys.stderr)
        return EXIT_ERROR


if __name__ == "__main__":
    raise SystemExit(main())
