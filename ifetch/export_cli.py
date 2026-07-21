#!/usr/bin/env python3
"""
iFetch Export CLI - Export local folders and iCloud Drive to Google Drive.

This module provides a command-line interface for exporting data from local
folders (Documents, Downloads, etc.) and iCloud Drive to Google Drive.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import List, Optional

from .exporters.googledrive import DEFAULT_TOKEN_FILE, GoogleDriveExporter


def get_default_folders() -> List[str]:
    """Get default macOS folders to export."""
    home = Path.home()
    folders = [
        home / 'Documents',
        home / 'Downloads',
        home / 'Desktop',
        home / 'Pictures',
    ]

    # Filter to only existing folders
    return [str(f) for f in folders if f.exists()]


def _stdin_is_interactive() -> bool:
    """Return True when stdin is attached to a TTY we can prompt on."""
    try:
        return bool(sys.stdin is not None and sys.stdin.isatty())
    except (AttributeError, ValueError):
        return False


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Export local folders and iCloud Drive data to Google Drive',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export default folders (Documents, Downloads, Desktop, Pictures)
  python -m ifetch.export_cli

  # Run unattended (cron/CI) without the confirmation prompt
  python -m ifetch.export_cli --yes

  # Export specific folders
  python -m ifetch.export_cli --folders ~/Documents ~/Downloads

  # Export with pattern filtering
  python -m ifetch.export_cli --include "*.pdf" "*.docx"

  # Export excluding certain patterns
  python -m ifetch.export_cli --exclude "*.tmp" "*.cache"

  # Force re-upload all files (even if unchanged)
  python -m ifetch.export_cli --force

  # Use custom Google Drive folder name
  python -m ifetch.export_cli --gdrive-folder "My Backup"

  # Use custom credentials file
  python -m ifetch.export_cli --credentials ~/my-credentials.json

  # Use custom ignore file
  python -m ifetch.export_cli --ignore-file ~/my-ignore-patterns.txt

  # Disable ignore file (upload everything)
  python -m ifetch.export_cli --no-ignore

  # Show index statistics
  python -m ifetch.export_cli --show-index-stats

  # Rebuild index from scratch
  python -m ifetch.export_cli --rebuild-index

  # Disable index (slower, rescans everything)
  python -m ifetch.export_cli --no-index
        """
    )

    parser.add_argument(
        '--folders',
        nargs='+',
        help='Folders to export (default: Documents, Downloads, Desktop, Pictures)',
        default=None
    )

    parser.add_argument(
        '--gdrive-folder',
        default='MacOS Data',
        help='Google Drive folder name to store exports (default: "MacOS Data")'
    )

    parser.add_argument(
        '--credentials',
        default='credentials.json',
        help='Path to Google OAuth2 credentials file (default: credentials.json)'
    )

    parser.add_argument(
        '--token',
        default=DEFAULT_TOKEN_FILE,
        help=f'Path to store Google Drive authentication token (default: {DEFAULT_TOKEN_FILE})'
    )

    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help='Skip the confirmation prompt (required for non-interactive runs)'
    )

    parser.add_argument(
        '--force',
        action='store_true',
        help='Force re-upload all files, even if unchanged'
    )

    parser.add_argument(
        '--include',
        nargs='+',
        help='Include only files matching these patterns (e.g., *.pdf *.docx)',
        default=None
    )

    parser.add_argument(
        '--exclude',
        nargs='+',
        help='Exclude files matching these patterns (e.g., *.tmp *.cache)',
        default=None
    )

    parser.add_argument(
        '--chunk-size',
        type=int,
        default=10,
        help='Upload chunk size in MB for resumable uploads (default: 10)'
    )

    parser.add_argument(
        '--upload-workers',
        type=int,
        default=4,
        help='Number of parallel upload workers for faster uploads (default: 4, max recommended: 8)'
    )

    parser.add_argument(
        '--ignore-file',
        default='.gdriveexportignore',
        help='Path to ignore file (like .gitignore) for excluding files/folders (default: .gdriveexportignore)'
    )

    parser.add_argument(
        '--no-ignore',
        action='store_true',
        help='Disable ignore file, upload everything'
    )

    parser.add_argument(
        '--index-file',
        default='.gdrive_upload_index.json',
        help='Path to index file for tracking uploads (default: .gdrive_upload_index.json)'
    )

    parser.add_argument(
        '--no-index',
        action='store_true',
        help='Disable index, rescan all files (slower but ensures fresh state)'
    )

    parser.add_argument(
        '--rebuild-index',
        action='store_true',
        help='Clear index and rebuild from scratch'
    )

    parser.add_argument(
        '--show-index-stats',
        action='store_true',
        help='Show index statistics and exit'
    )

    parser.add_argument(
        '--list-defaults',
        action='store_true',
        help='List default folders that would be exported and exit'
    )

    return parser.parse_args()


def main():
    """Main entry point for export CLI."""
    args = parse_args()

    # Handle index statistics command
    if args.show_index_stats:
        from .exporters.index_manager import UploadIndexManager
        index_manager = UploadIndexManager(args.index_file)
        index_manager.print_stats()
        return 0

    # Handle rebuild index command
    if args.rebuild_index:
        from .exporters.index_manager import UploadIndexManager
        print(f"Rebuilding index: {args.index_file}")
        index_manager = UploadIndexManager(args.index_file)
        index_manager.rebuild_index()
        print("Index has been cleared and will be rebuilt on next upload.")
        return 0

    # List default folders if requested
    if args.list_defaults:
        print("Default folders to export:")
        for folder in get_default_folders():
            print(f"  - {folder}")
        return 0

    # Determine folders to export
    folders = args.folders if args.folders else get_default_folders()

    if not folders:
        print("Error: No folders to export. Specify --folders or ensure default folders exist.")
        return 1

    print(f"\n{'='*60}")
    print("iFetch Google Drive Export")
    print(f"{'='*60}\n")

    print(f"Google Drive folder: {args.gdrive_folder}")
    print(f"Credentials file: {args.credentials}")
    print(f"Folders to export ({len(folders)}):")
    for folder in folders:
        print(f"  - {folder}")

    if args.include:
        print(f"Include patterns: {', '.join(args.include)}")
    if args.exclude:
        print(f"Exclude patterns: {', '.join(args.exclude)}")
    if args.no_ignore:
        print("Ignore file: Disabled")
    else:
        print(f"Ignore file: {args.ignore_file}")
    if args.no_index:
        print("Index: Disabled (will rescan all files)")
    else:
        print(f"Index file: {args.index_file}")
    if args.force:
        print("Mode: Force re-upload all files")
    else:
        print("Mode: Skip unchanged files")

    print()

    # Prompt for confirmation (unless explicitly confirmed on the command line)
    if not args.yes:
        if not _stdin_is_interactive():
            print(
                "Error: cannot prompt for confirmation because stdin is not a terminal.\n"
                "Re-run with --yes/-y to confirm non-interactively.",
                file=sys.stderr,
            )
            return 1
        try:
            response = input("Continue with export? [y/N]: ").strip().lower()
            if response not in ['y', 'yes']:
                print("Export cancelled.")
                return 0
        except KeyboardInterrupt:
            print("\nExport cancelled.")
            return 0
        except EOFError:
            print(
                "\nError: no input available to confirm the export. "
                "Re-run with --yes/-y to confirm non-interactively.",
                file=sys.stderr,
            )
            return 1

    # Initialize Google Drive exporter
    try:
        exporter = GoogleDriveExporter(
            credentials_file=args.credentials,
            token_file=args.token,
            root_folder_name=args.gdrive_folder,
            chunk_size=args.chunk_size * 1024 * 1024,  # Convert MB to bytes
            ignore_file=None if args.no_ignore else args.ignore_file,
            index_file=args.index_file,
            use_index=not args.no_index,
            upload_workers=args.upload_workers
        )

        # Authenticate
        print("\nAuthenticating with Google Drive...")
        exporter.authenticate()

        # Export folders
        stats = exporter.export_local_folders(
            folders=folders,
            force=args.force,
            include_patterns=args.include,
            exclude_patterns=args.exclude
        )

        # Print summary
        exporter.print_summary(stats)

        # Return non-zero exit code if there were failures
        return 1 if stats['failed'] > 0 else 0

    except FileNotFoundError as e:
        print(f"\nError: {e}")
        return 1
    except KeyboardInterrupt:
        print("\n\nExport interrupted by user.")
        return 130
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
