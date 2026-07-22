#!/usr/bin/env python3
import sys
import argparse
from pathlib import Path

# ---------------------------------------------------------------------------
# Import DownloadManager whether this script is executed as a module inside
# the ifetch package or run directly via `python ifetch/cli.py`.
# ---------------------------------------------------------------------------

if __package__ in (None, ""):
    # Running as a standalone script: add project root to path and import absolute
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.downloader import DownloadManager  # type: ignore
else:
    # Running as part of package (python -m ifetch.cli)
    from .downloader import DownloadManager  # type: ignore


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    # `ifetch auth ...` is a distinct command tree rather than a flag soup on
    # the download parser.  Dispatching before argparse runs keeps the two from
    # having to share options that mean different things.
    if argv and argv[0] == 'auth':
        if __package__ in (None, ""):
            from ifetch.auth_cli import main as auth_main  # type: ignore
        else:
            from .auth_cli import main as auth_main  # type: ignore
        return sys.exit(auth_main(argv[1:]))

    parser = argparse.ArgumentParser(
        description=(
            'Sync files/folders from iCloud Drive locally with resume, diff, '
            'and parallel downloads. Run "ifetch auth doctor" to diagnose '
            'authentication problems.'
        )
    )
    parser.add_argument(
        'icloud_path',
        nargs='?',
        default=None,
        help='Remote iCloud Drive path (e.g., "Documents/MyFolder"). Required unless --list-shared is supplied.'
    )
    parser.add_argument(
        'local_path',
        nargs='?',
        default='.',
        help='Local destination directory (default: current directory)'
    )
    parser.add_argument(
        '--email',
        help='iCloud account email (can also use ICLOUD_EMAIL environment variable)'
    )
    parser.add_argument(
        '--max-workers',
        type=int,
        default=4,
        help='Maximum number of concurrent downloads (default: 4)'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum number of retry attempts for failed chunks (default: 3)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1024*1024,
        help='Chunk size in bytes for differential downloads (default: 1MB)'
    )
    parser.add_argument(
        '--log-file',
        help='Path to a file to save structured JSON logs'
    )
    parser.add_argument(
        '--list',
        dest='list_only',
        action='store_true',
        help='List directory contents instead of downloading'
    )

    parser.add_argument(
        '--list-shared',
        dest='list_shared',
        action='store_true',
        help='List top-level items that have been shared with you'
    )

    parser.add_argument(
        '--no-fast-scan',
        dest='fast_scan',
        action='store_false',
        default=True,
        help=(
            'Disable the metadata fast path. Every file is opened over the network '
            'to check its size, even when local sync state says it is unchanged.'
        )
    )

    parser.add_argument(
        '--force',
        dest='force',
        action='store_true',
        help='Re-download every file regardless of local state or size match'
    )

    parser.add_argument(
        '--profile',
        help='Profile name from ~/.ifetch_profiles.json to use for include/exclude patterns'
    )

    parser.add_argument(
        '--profile-file',
        dest='profile_file',
        help='Custom path to a profile JSON file (overrides default ~/.ifetch_profiles.json)'
    )

    parser.add_argument(
        '--region',
        choices=['global', 'china'],
        default=None,
        help=(
            'iCloud region. "china" switches every Apple endpoint to '
            'iCloud.com.cn, which China Mainland Apple IDs require. Defaults to '
            '$ICLOUD_REGION, then the legacy ICLOUD_CHINA=true, then "global".'
        )
    )

    parser.add_argument(
        '--no-expand-packages',
        dest='expand_packages',
        action='store_false',
        default=True,
        help=(
            'Write Apple package bundles (.key/.pages/.numbers/.xcodeproj/...) '
            'as the raw ZIP archive Apple serves, instead of expanding them '
            'back into usable directories.'
        )
    )

    manifest_group = parser.add_argument_group(
        'integrity manifest',
        'Apple publishes no content hashes, so iFetch records its own at '
        'download time. ifetch-verify --offline replays them to detect bit-rot.'
    )
    manifest_group.add_argument(
        '--sign-key',
        dest='sign_key',
        help='HMAC key used to sign the manifest (or set $IFETCH_MANIFEST_KEY)'
    )
    manifest_group.add_argument(
        '--sign-key-file',
        dest='sign_key_file',
        help='Read the manifest signing key from this file'
    )

    twofa_group = parser.add_argument_group(
        'non-interactive two-factor',
        'Answer Apple\'s 2FA challenge without a terminal, for cron/Docker/NAS.'
    )
    twofa_group.add_argument('--2fa-code', dest='twofa_code', help='The six-digit code')
    twofa_group.add_argument(
        '--2fa-file', dest='twofa_file',
        help='Poll this file until it contains a six-digit code'
    )
    twofa_group.add_argument(
        '--2fa-webhook', dest='twofa_webhook',
        help='Poll this URL (GET) until the response contains a six-digit code'
    )
    twofa_group.add_argument(
        '--2fa-timeout', dest='twofa_timeout', type=float, default=300.0,
        help='Seconds to wait for a polled code (default: 300)'
    )

    parser.add_argument(
        '--warn-days',
        type=int,
        default=7,
        help=(
            'Warn before starting if the iCloud session expires within this '
            'many days (default: 7). Apple trust tokens last about 30.'
        )
    )

    args = parser.parse_args(argv)

    try:
        # Create a progress banner
        print("=" * 70)
        print(f"iCloud Drive Downloader")
        if args.icloud_path:
            print(f"Remote Path: {args.icloud_path}")
        print(f"Local Path: {args.local_path}")
        print(f"Parallel Workers: {args.max_workers}")
        print("=" * 70)

        # Load profile patterns
        from ifetch.profiles import ProfileManager  # local import to avoid overhead if unused

        pm = None
        if args.profile:
            from pathlib import Path as _P
            cfg_path = _P(args.profile_file).expanduser() if args.profile_file else None
            pm = ProfileManager(args.profile, config_path=cfg_path)  # type: ignore[arg-type]
        include_pats, exclude_pats = pm.get_patterns() if pm else ([], [])

        if __package__ in (None, ""):
            from ifetch.auth import TwoFactorResolver  # type: ignore
            from ifetch.manifest import load_signing_key  # type: ignore
        else:
            from .auth import TwoFactorResolver  # type: ignore
            from .manifest import load_signing_key  # type: ignore

        manifest_key = load_signing_key(
            key=args.sign_key,
            key_file=Path(args.sign_key_file).expanduser() if args.sign_key_file else None,
        )

        # Initialize the downloader
        downloader = DownloadManager(
            email=args.email,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            chunk_size=args.chunk_size,
            include_patterns=include_pats,
            exclude_patterns=exclude_pats,
            fast_scan=getattr(args, 'fast_scan', True),
            force=getattr(args, 'force', False),
            region=args.region,
            expand_packages=args.expand_packages,
            manifest_key=manifest_key,
            warn_days=args.warn_days,
        )

        two_factor = TwoFactorResolver(
            code=args.twofa_code,
            file=Path(args.twofa_file).expanduser() if args.twofa_file else None,
            webhook=args.twofa_webhook,
            timeout=args.twofa_timeout,
        )

        # Authenticate (will prompt for password if needed).
        # `authenticate` is a documented extension point that subclasses and
        # plugins override, and `two_factor` is a newer keyword. Fall back to
        # the no-argument form so an override written against the older
        # signature keeps working instead of dying on an unexpected kwarg.
        print("Authenticating with iCloud...")
        try:
            downloader.authenticate(two_factor=two_factor)
        except TypeError as exc:
            if 'two_factor' not in str(exc):
                raise
            downloader.authenticate()
        print("Authentication successful!")

        # Perform the requested operation
        if args.list_shared:
            print("\nListing top-level shared items:")
            print("-" * 50)
            downloader.list_shared_roots()
        elif args.list_only:
            print(f"\nListing contents of '{args.icloud_path}':")
            print("-" * 50)
            downloader.list_contents(args.icloud_path)
        else:
            if not args.icloud_path:
                raise ValueError("icloud_path is required unless using --list-shared")
            print(f"\nDownloading from '{args.icloud_path}' to '{args.local_path}'")
            print("This may take some time depending on the size of the content...")
            downloader.download(
                args.icloud_path,
                args.local_path,
                log_file=args.log_file
            )

            # Show a summary after download completes
            summary = downloader.generate_summary_report()["summary"]
            print("\nDownload Summary:")
            print(f"- Total files: {summary['total_files']}")
            print(f"- Successfully downloaded: {summary['successful']}")
            print(f"- Failed: {summary['failed']}")
            print(f"- Skipped (unchanged): {summary.get('skipped', 0)}")
            print(f"- Total data transferred: {summary['total_bytes_transferred'] / (1024*1024):.2f} MB")
            print(f"- Changed chunks: {summary['total_changed_chunks']}")
            print(f"\nDetailed report saved to '{args.local_path}/download_report.json'")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nOperation completed.")

if __name__ == '__main__':
    main()
