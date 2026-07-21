#!/usr/bin/env python3
"""
iFetch Photos CLI - bulk download your iCloud Photos library.

Downloads originals by default, skips anything already fetched (via a local
`.ifetch_photos_index.json` index), and organises the result either flat or
by capture date. Live Photo video components can be paired with `--live-photos`.
"""

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Import PhotosDownloader whether this script is executed as a module inside
# the ifetch package or run directly via `python ifetch/photos_cli.py`.
# ---------------------------------------------------------------------------

if __package__ in (None, ""):
    # Running as a standalone script: add project root to path and import absolute
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from ifetch.photos import (  # type: ignore
        FOLDER_STRUCTURES,
        PhotosDownloader,
        parse_date,
    )
else:
    # Running as part of package (python -m ifetch.photos_cli)
    from .photos import FOLDER_STRUCTURES, PhotosDownloader, parse_date  # type: ignore


def parse_args(argv=None):
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        prog='ifetch-photos',
        description=(
            'Download your iCloud Photos library locally: originals by default, '
            'delta-aware (already-downloaded assets are skipped), parallel, resumable.'
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download the whole library into ./Photos, organised as YYYY/MM
  ifetch-photos ./Photos

  # Just one album, flat layout
  ifetch-photos ./Photos --album "Family" --folder-structure flat

  # Everything shot in 2024, with the Live Photo videos
  ifetch-photos ./Photos --since 2024-01-01 --until 2024-12-31 --live-photos

  # See what a run would do without downloading anything
  ifetch-photos ./Photos --dry-run

  # What albums exist?
  ifetch-photos --list-albums
        """
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
        '--album',
        default=None,
        help='Album name to download (default: the whole library)'
    )
    parser.add_argument(
        '--since',
        default=None,
        metavar='DATE',
        help='Only assets captured on or after this date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--until',
        default=None,
        metavar='DATE',
        help='Only assets captured on or before this date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--folder-structure',
        dest='folder_structure',
        default='date',
        choices=list(FOLDER_STRUCTURES),
        help='Local layout: flat, year (YYYY), date (YYYY/MM), day (YYYY/MM/DD) '
             'or album (default: date)'
    )
    parser.add_argument(
        '--version',
        dest='version',
        default='original',
        help='Asset derivative to download: original, medium or thumb (default: original)'
    )
    parser.add_argument(
        '--live-photos',
        dest='live_photos',
        action='store_true',
        help='Also download the paired video component of Live Photos'
    )
    parser.add_argument(
        '--include-deleted',
        dest='include_deleted',
        action='store_true',
        help='Also download the "Recently Deleted" album, when the API exposes it'
    )
    parser.add_argument(
        '--no-set-mtime',
        dest='set_mtime',
        action='store_false',
        help='Do not set local file modification times to the capture date'
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
        help='Maximum number of retry attempts per asset (default: 3)'
    )
    parser.add_argument(
        '--log-file',
        dest='log_file',
        help='Path to a file to save structured JSON logs'
    )
    parser.add_argument(
        '--list-albums',
        dest='list_albums',
        action='store_true',
        help='List available photo albums and exit'
    )
    parser.add_argument(
        '--dry-run',
        dest='dry_run',
        action='store_true',
        help='Show what would be downloaded without writing any files'
    )

    return parser.parse_args(argv)


def main(argv=None) -> int:
    """Main entry point for the photos CLI."""
    args = parse_args(argv)

    try:
        since = parse_date(args.since) if args.since else None
        until = parse_date(args.until) if args.until else None

        downloader = PhotosDownloader(
            email=args.email,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            version=args.version,
            folder_structure=args.folder_structure,
            live_photos=args.live_photos,
            include_deleted=args.include_deleted,
            dry_run=args.dry_run,
            set_mtime=args.set_mtime,
        )

        print("=" * 70)
        print("iCloud Photos Downloader")
        if not args.list_albums:
            print(f"Album: {args.album or 'All Photos'}")
            print(f"Local Path: {args.local_path}")
            print(f"Layout: {args.folder_structure}")
            print(f"Parallel Workers: {args.max_workers}")
            if args.dry_run:
                print("Mode: dry-run")
        print("=" * 70)

        print("Authenticating with iCloud...")
        downloader.authenticate()
        print("Authentication successful!")

        if args.list_albums:
            albums = downloader.list_albums()
            print("\nAlbums:")
            print("-" * 50)
            for album in albums:
                count = album.get("count")
                suffix = f" ({count} items)" if isinstance(count, int) else ""
                print(f"- {album['fullname']}{suffix}")
            print(f"\n{len(albums)} album(s).")
            return 0

        report = downloader.download(
            destination=args.local_path,
            album=args.album,
            since=since,
            until=until,
            log_file=args.log_file,
        )

        summary = report["summary"]
        print("\nPhotos Summary:")
        print(f"- Total assets considered: {summary['total_assets']}")
        if args.dry_run:
            print(f"- Would download: {summary.get('would_download', 0)}")
        else:
            print(f"- Downloaded: {summary['downloaded']}")
        print(f"- Skipped (already downloaded): {summary['skipped']}")
        print(f"- Failed: {summary['failed']}")
        print(
            f"- Total data transferred: "
            f"{summary['total_bytes_transferred'] / (1024 * 1024):.2f} MB"
        )
        if not args.dry_run:
            print(f"\nDetailed report saved to '{args.local_path}/photos_report.json'")

        return 1 if summary["failed"] else 0

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
