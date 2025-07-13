#!/usr/bin/env python3
import sys
import argparse
from ifetch.downloader import DownloadManager

def main():
    parser = argparse.ArgumentParser(
        description='Sync files/folders from iCloud Drive locally with resume, diff, and parallel downloads.'
    )
    parser.add_argument(
        'icloud_path',
        help='Remote iCloud Drive path (e.g., "Documents/MyFolder")'
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

    args = parser.parse_args()

    try:
        # Create a progress banner
        print("=" * 70)
        print(f"iCloud Drive Downloader")
        print(f"Remote Path: {args.icloud_path}")
        print(f"Local Path: {args.local_path}")
        print(f"Parallel Workers: {args.max_workers}")
        print("=" * 70)

        # Initialize the downloader
        downloader = DownloadManager(
            email=args.email,
            max_workers=args.max_workers,
            max_retries=args.max_retries,
            chunk_size=args.chunk_size
        )

        # Authenticate (will prompt for password if needed)
        print("Authenticating with iCloud...")
        downloader.authenticate()
        print("Authentication successful!")

        # Perform the requested operation
        if args.list_only:
            print(f"\nListing contents of '{args.icloud_path}':")
            print("-" * 50)
            downloader.list_contents(args.icloud_path)
        else:
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
