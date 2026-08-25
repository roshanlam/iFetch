#!/usr/bin/env python3
import sys
import argparse
from importlib import import_module
from pathlib import Path

# ---------------------------------------------------------------------------
# Import DownloadManager whether this script is executed as a module inside
# the ifetch package or run directly via `python ifetch/cli.py`.
# ---------------------------------------------------------------------------

if __package__ in (None, ""):
    # Running as a standalone script: add project root to path and import absolute
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.downloader import DownloadManager  # type: ignore
    from ifetch.ratelimit import BandwidthLimitError, parse_bwlimit  # type: ignore
    from ifetch.notify import add_notification_arguments, build_notifier  # type: ignore
else:
    # Running as part of package (python -m ifetch.cli)
    from .downloader import DownloadManager  # type: ignore
    from .ratelimit import BandwidthLimitError, parse_bwlimit  # type: ignore
    from .notify import add_notification_arguments, build_notifier  # type: ignore


#: ``ifetch <name>`` -> the module and function that implements it. Imported
#: lazily so a plain download does not pay for parsers it will not use.
SUBCOMMANDS = {
    "auth": ("auth_cli", "main"),
    "snapshot": ("snapshot_cli", "main"),
    "repair": ("repair_cli", "main"),
    "resume": ("repair_cli", "resume_main"),
    "conflicts": ("conflict_cli", "main"),
    "recover": ("recover_cli", "main"),
    "plan": ("plan_cli", "main"),
    "audit": ("plan_cli", "audit_main"),
    "guard": ("guard_cli", "main"),
    "vanish": ("vanish_cli", "main"),
    "sharecheck": ("sharecheck_cli", "main"),
    "uplink": ("uplink_cli", "main"),
    "serve": ("serve_cli", "main"),
}


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    # Each of these is its own command tree rather than more flags on the
    # download parser, and each is dispatched before argparse runs so the two
    # never have to share an option that means different things in each.
    if argv and argv[0] in SUBCOMMANDS:
        module_name, entry_name = SUBCOMMANDS[argv[0]]
        module = import_module(f"{__package__ or 'ifetch'}.{module_name}")
        return sys.exit(getattr(module, entry_name)(argv[1:]))

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
        '--skip-existing',
        dest='skip_existing',
        action='store_true',
        help='Skip files that already exist locally instead of updating/overwriting them'
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
        '--password-command',
        dest='password_command',
        help=(
            'Shell-free command that prints the Apple ID password on stdout, for '
            'machines with no system keyring (Docker, systemd, NAS). Quote paths '
            'containing spaces. Or set $IFETCH_PASSWORD_COMMAND.'
        )
    )

    naming_group = parser.add_argument_group(
        'filename handling',
        'Apple returns names in Unicode NFD and permits characters some '
        'filesystems cannot store.'
    )
    naming_group.add_argument(
        '--portable-names',
        dest='portable_names',
        action='store_true',
        help=(
            'Sanitize names for the strictest common filesystem rather than just '
            'the current one. Use when the destination is exFAT, a NAS share, or '
            'will be read from Windows.'
        )
    )
    naming_group.add_argument(
        '--normalize-names',
        dest='normalize_names',
        choices=['preserve', 'nfc'],
        default='preserve',
        help=(
            'Local filename spelling. "preserve" (default) keeps Apple\'s exact '
            'bytes. "nfc" writes composed Unicode, which is more natural to type '
            'on Linux -- but it renames existing files, forcing a re-download of '
            'anything already mirrored under the decomposed spelling.'
        )
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

    parser.add_argument(
        '--bwlimit',
        dest='bwlimit',
        default=None,
        metavar='RATE|TIMETABLE',
        help=(
            'Cap total download bandwidth, rclone-style. Either one rate '
            '("512k", "10M") or a timetable ("08:00,512k 12:00,10M 13:00,off '
            '18:00,30M 23:00,off"), optionally with weekday prefixes '
            '("Mon-08:00,512k"). Units are binary and a bare number means '
            'KiB/s, as in rclone; "off" or 0 is unlimited. The cap applies to '
            'the whole run, not to each worker, and is re-read as the clock '
            'crosses a boundary so a limit change takes effect without '
            'restarting. Default: unlimited.'
        )
    )

    parser.add_argument(
        '--retry-failed',
        dest='retry_failed',
        nargs='?',
        const='',
        default=None,
        help='Retry ONLY the files marked "failed" in a prior report, skipping the '
             'full-tree walk and re-verification of good files. Optionally give a '
             'report path (default: <local_path>/download_report.json). Requires the '
             'same icloud_path (remote root) and local_path as the original run.'
    )

    add_notification_arguments(parser)

    args = parser.parse_args(argv)

    # A notifier is always constructed; with nothing configured it is a no-op,
    # so the call sites below need no conditionals and an unattended run that
    # nobody is watching costs nothing.
    notifier = build_notifier(args)

    # Reject a malformed limit before the banner prints and before any network
    # I/O, so an overnight job fails in the first second rather than at the
    # first byte.
    if args.bwlimit is not None:
        try:
            parse_bwlimit(args.bwlimit)
        except BandwidthLimitError as exc:
            parser.error(str(exc))

    try:
        # Create a progress banner
        print("=" * 70)
        print(f"iCloud Drive Downloader")
        if args.icloud_path:
            print(f"Remote Path: {args.icloud_path}")
        print(f"Local Path: {args.local_path}")
        print(f"Parallel Workers: {args.max_workers}")
        if args.skip_existing:
            print("Skip-existing: ON (files already on disk will be left untouched)")
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
            from ifetch.auth import TwoFactorResolver, resolve_password  # type: ignore
            from ifetch.manifest import load_signing_key  # type: ignore
        else:
            from .auth import TwoFactorResolver, resolve_password  # type: ignore
            from .manifest import load_signing_key  # type: ignore

        password = resolve_password(args.password_command)

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
            portable_names=args.portable_names,
            normalize_names=args.normalize_names,
            password=password,
            bwlimit=args.bwlimit,
            skip_existing=args.skip_existing,
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
        notifier.start(
            icloud_path=args.icloud_path,
            local_path=str(args.local_path),
        )
        print("Authenticating with iCloud...")
        try:
            downloader.authenticate(two_factor=two_factor)
        except TypeError as exc:
            if 'two_factor' not in str(exc):
                raise
            downloader.authenticate()
        print("Authentication successful!")

        # Perform the requested operation
        if args.retry_failed is not None:
            if not args.icloud_path:
                raise ValueError("icloud_path (remote root) is required with --retry-failed")
            from pathlib import Path as _P
            report = args.retry_failed or str(_P(args.local_path) / "download_report.json")
            print(f"\nRetrying failed entries from '{report}'")
            downloader.retry_failed(
                report,
                args.icloud_path,
                args.local_path,
                log_file=args.log_file,
            )
            summary = downloader.generate_summary_report()["summary"]
            print("\nRetry Summary:")
            print(f"- Files retried: {summary['total_files']}")
            print(f"- Successfully downloaded: {summary['successful']}")
            print(f"- Still failed: {summary['failed']}")
            print(f"- Total data transferred: {summary['total_bytes_transferred'] / (1024*1024):.2f} MB")
        elif args.list_shared:
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
            full_report = downloader.generate_summary_report()
            # Sends success, and separately an anomaly if the run finished but
            # some files failed. Conflating those two is how people learn to
            # ignore alerts.
            notifier.run_finished(full_report)
            summary = full_report["summary"]
            print("\nDownload Summary:")
            print(f"- Total files: {summary['total_files']}")
            print(f"- Successfully downloaded: {summary['successful']}")
            print(f"- Skipped (already on disk): {summary['skipped']}")
            print(f"- Failed: {summary['failed']}")
            print(f"- Skipped (unchanged): {summary.get('skipped', 0)}")
            print(f"- Total data transferred: {summary['total_bytes_transferred'] / (1024*1024):.2f} MB")
            print(f"- Changed chunks: {summary['total_changed_chunks']}")
            print(f"\nDetailed report saved to '{args.local_path}/download_report.json'")

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        notifier.failure(e)
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print("\nOperation completed.")

if __name__ == '__main__':
    main()
