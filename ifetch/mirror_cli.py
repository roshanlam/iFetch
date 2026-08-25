#!/usr/bin/env python3
"""
iFetch Mirror CLI - one-command pipeline: iCloud Drive -> local folder -> Google Drive.

Stage 1 (always): sync an iCloud Drive path to a local folder (delta-aware).
Stage 2 (with --gdrive-folder): upload that local folder to Google Drive
(delta-aware via the upload index). Stage 2 is skipped when stage 1 had
errors, so a partial local state is never mirrored onward.

With --watch N the pipeline repeats every N seconds until interrupted.
"""

import argparse
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Import MirrorPipeline whether this script is executed as a module inside
# the ifetch package or run directly via `python ifetch/mirror_cli.py`.
# ---------------------------------------------------------------------------

if __package__ in (None, ""):
    # Running as a standalone script: add project root to path and import absolute
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.mirror import MirrorPipeline  # type: ignore
else:
    # Running as part of package (python -m ifetch.mirror_cli)
    from .mirror import MirrorPipeline  # type: ignore


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            'Mirror iCloud Drive to a local folder (e.g. a NAS mount) and, '
            'optionally, on to Google Drive. Delta-aware at both hops.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mirror an iCloud folder to a NAS mount
  ifetch-mirror "Documents/Projects" /Volumes/nas/icloud

  # Mirror all the way to Google Drive
  ifetch-mirror "Documents" /Volumes/nas/icloud --gdrive-folder "iCloud Mirror"

  # Keep it running, syncing every hour
  ifetch-mirror "Documents" /Volumes/nas/icloud --gdrive-folder "iCloud Mirror" --watch 3600

  # See what would happen without transferring anything
  ifetch-mirror "Documents" /Volumes/nas/icloud --gdrive-folder "iCloud Mirror" --dry-run
        """
    )

    parser.add_argument(
        'icloud_path',
        help='Remote iCloud Drive path (e.g., "Documents/MyFolder")'
    )
    parser.add_argument(
        'local_path',
        help='Local destination directory (e.g., a NAS mount point)'
    )
    parser.add_argument(
        '--gdrive-folder',
        default=None,
        help='Google Drive folder name to mirror the local folder into '
             '(stage 2 runs only when this is given)'
    )
    parser.add_argument(
        '--watch',
        type=float,
        default=None,
        metavar='SECONDS',
        help='Repeat the pipeline every SECONDS seconds until interrupted'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List what stage 1 would download and skip stage 2 uploads'
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
        '--profile',
        help='Profile name from ~/.ifetch_profiles.json to use for include/exclude patterns'
    )
    parser.add_argument(
        '--profile-file',
        dest='profile_file',
        help='Custom path to a profile JSON file (overrides default ~/.ifetch_profiles.json)'
    )
    parser.add_argument(
        '--credentials',
        default='credentials.json',
        help='Path to Google OAuth2 credentials file (default: credentials.json)'
    )
    parser.add_argument(
        '--token',
        default='.gdrive_token.json',
        help='Path to store Google Drive authentication token (default: .gdrive_token.json)'
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Main entry point for the mirror CLI."""
    args = parse_args(argv)

    try:
        print("=" * 70)
        print("iFetch Mirror: iCloud Drive -> local" +
              (" -> Google Drive" if args.gdrive_folder else ""))
        print(f"Remote Path: {args.icloud_path}")
        print(f"Local Path: {args.local_path}")
        if args.gdrive_folder:
            print(f"Google Drive Folder: {args.gdrive_folder}")
        if args.watch is not None:
            print(f"Watch Interval: {args.watch:g}s")
        if args.dry_run:
            print("Mode: dry-run")
        print("=" * 70)

        # Load profile patterns (same behaviour as ifetch.cli)
        from ifetch.profiles import ProfileManager  # local import to avoid overhead if unused

        pm = None
        if args.profile:
            cfg_path = Path(args.profile_file).expanduser() if args.profile_file else None
            pm = ProfileManager(args.profile, config_path=cfg_path)  # type: ignore[arg-type]
        include_pats, exclude_pats = pm.get_patterns() if pm else ([], [])

        pipeline = MirrorPipeline(
            icloud_path=args.icloud_path,
            local_path=args.local_path,
            gdrive_folder=args.gdrive_folder,
            email=args.email,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            chunk_size=args.chunk_size,
            include_patterns=include_pats,
            exclude_patterns=exclude_pats,
            log_file=args.log_file,
            credentials_file=args.credentials,
            token_file=args.token,
            dry_run=args.dry_run,
        )
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    # ------------------------------------------------------------------
    # Watch mode: loop forever, containing per-cycle failures.
    # ------------------------------------------------------------------
    if args.watch is not None:
        cycle = 1
        try:
            while True:
                try:
                    result = pipeline.run_once(cycle=cycle)
                    print(result.summary_line())
                    for error in result.errors:
                        print(f"[cycle {cycle}] error: {error}", file=sys.stderr)
                except KeyboardInterrupt:
                    raise
                except Exception as e:  # noqa: BLE001 - one bad cycle must not kill the loop
                    print(f"[cycle {cycle}] cycle failed: {e}", file=sys.stderr)
                cycle += 1
                time.sleep(args.watch)
        except KeyboardInterrupt:
            print("\nWatch loop stopped by user.")
            return 0

    # ------------------------------------------------------------------
    # Single-shot mode.
    # ------------------------------------------------------------------
    try:
        result = pipeline.run_once()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 130

    print(result.summary_line())
    for error in result.errors:
        print(f"Error: {error}", file=sys.stderr)

    return 0 if result.success else 1


if __name__ == '__main__':
    sys.exit(main())
