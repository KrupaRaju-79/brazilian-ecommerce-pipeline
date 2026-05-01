"""
scripts/fix_reviews_csv.py
==========================
Cleans olist_order_reviews_dataset.csv before BigQuery load.

ROOT CAUSE
----------
The reviews CSV has two problems that make bq load fail:
  1. Unquoted commas inside review_comment_message → "Found 8 columns when expecting 7"
  2. Unquoted newlines inside review_comment_message → row splits across lines

STRATEGY
---------
A valid review row has a fixed structure with known-format anchors:
  col 0: review_id       → 32-char hex UUID
  col 1: order_id        → 32-char hex UUID
  col 2: review_score    → integer 1–5
  col 3: title           → free text (often empty)
  col 4: message         → free text ← the problem column
  col 5: creation_date   → YYYY-MM-DD HH:MM:SS
  col 6: answer_ts       → YYYY-MM-DD HH:MM:SS

Fix: for rows with wrong column count, anchor on cols 0/1/2 (UUID+score)
and cols 5/6 (last two timestamps), then rejoin everything in between
as the message with commas restored.

USAGE
-----
  pip install google-cloud-storage
  python scripts/fix_reviews_csv.py --project_id=YOUR_PROJECT_ID --bucket=YOUR_BUCKET

  # Or with local input/output only (no GCS upload):
  python scripts/fix_reviews_csv.py --local_only \
    --input_csv=data/olist_order_reviews_dataset.csv \
    --output_csv=data/olist_order_reviews_clean.csv
"""

import argparse
import csv
import io
import os
import re
import sys
from datetime import datetime


# ─────────────────────────────────────────────────────────────────────────────
# Row validation helpers
# ─────────────────────────────────────────────────────────────────────────────
def is_uuid(s: str) -> bool:
    return bool(re.match(r'^[a-f0-9]{32}$', s.strip()))


def is_timestamp(s: str) -> bool:
    return bool(re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', s.strip()))


def is_score(s: str) -> bool:
    return s.strip() in ('1', '2', '3', '4', '5')


# ─────────────────────────────────────────────────────────────────────────────
# Row reconstruction
# ─────────────────────────────────────────────────────────────────────────────
def reconstruct_row(fragments: list) -> list | None:
    """
    Reconstruct a malformed row using anchor columns.
    Returns a clean 7-element list or None if unrecoverable.
    """
    if len(fragments) < 3:
        return None
    if not is_uuid(fragments[0]) or not is_uuid(fragments[1]):
        return None
    if not is_score(fragments[2]):
        return None

    # Find all timestamp positions
    ts_positions = [i for i, v in enumerate(fragments) if is_timestamp(v)]

    if len(ts_positions) >= 2:
        cd_idx = ts_positions[-2]   # creation_date = second-to-last timestamp
        at_idx = ts_positions[-1]   # answer_ts     = last timestamp
        middle = fragments[3:cd_idx]
        title  = middle[0].strip() if middle else ''
        msg    = ', '.join(v.strip() for v in middle[1:]) if len(middle) > 1 else ''
        return [
            fragments[0].strip(),
            fragments[1].strip(),
            fragments[2].strip(),
            title,
            msg,
            fragments[cd_idx].strip(),
            fragments[at_idx].strip(),
        ]

    elif len(ts_positions) == 1:
        # Only creation_date, no answer_ts
        cd_idx = ts_positions[0]
        middle = fragments[3:cd_idx]
        title  = middle[0].strip() if middle else ''
        msg    = ', '.join(v.strip() for v in middle[1:]) if len(middle) > 1 else ''
        return [
            fragments[0].strip(),
            fragments[1].strip(),
            fragments[2].strip(),
            title,
            msg,
            fragments[cd_idx].strip(),
            '',
        ]

    return None  # no timestamps found — unrecoverable


# ─────────────────────────────────────────────────────────────────────────────
# Main cleaning function
# ─────────────────────────────────────────────────────────────────────────────
def clean_reviews_csv(input_path: str, output_path: str) -> dict:
    """
    Read input CSV, clean all rows, write output CSV.
    Returns a stats dict.
    """
    print(f"  Reading:  {input_path}")

    with open(input_path, encoding='utf-8') as f:
        raw = f.read()

    reader    = csv.reader(io.StringIO(raw))
    header    = next(reader)

    if len(header) != 7:
        raise ValueError(f"Expected 7 header columns, got {len(header)}: {header}")

    clean_rows   = []
    already_good = 0
    reconstructed = 0
    skipped      = 0

    for row in reader:
        if len(row) == 7 and is_uuid(row[0]) and is_uuid(row[1]) and is_score(row[2]):
            clean_rows.append(row)
            already_good += 1

        elif len(row) != 7:
            fixed = reconstruct_row(row)
            if fixed:
                clean_rows.append(fixed)
                reconstructed += 1
            else:
                skipped += 1

        else:
            # 7 cols but UUID/score checks failed — keep but flag
            clean_rows.append(row)
            already_good += 1

    # Write output
    print(f"  Writing:  {output_path}")
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)   # quote EVERYTHING — no more unescaped commas
        writer.writerow(header)
        writer.writerows(clean_rows)

    stats = {
        'total_output':   len(clean_rows),
        'already_good':   already_good,
        'reconstructed':  reconstructed,
        'skipped':        skipped,
        'recovery_rate':  round(len(clean_rows) / (len(clean_rows) + skipped) * 100, 1),
    }
    return stats


# ─────────────────────────────────────────────────────────────────────────────
# GCS upload
# ─────────────────────────────────────────────────────────────────────────────
def upload_to_gcs(local_path: str, bucket_name: str, gcs_path: str):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path, content_type='text/csv')
    print(f"  Uploaded: gs://{bucket_name}/{gcs_path}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Clean reviews CSV for BigQuery load")
    parser.add_argument('--project_id',   help='GCP project ID')
    parser.add_argument('--bucket',       help='GCS bucket name (e.g. project-data-lake)')
    parser.add_argument('--input_csv',    default='data/olist_order_reviews_dataset.csv')
    parser.add_argument('--output_csv',   default='data/olist_order_reviews_clean.csv')
    parser.add_argument('--gcs_dest',     default='raw/olist_order_reviews_dataset.csv',
                        help='GCS path to overwrite (default: raw/olist_order_reviews_dataset.csv)')
    parser.add_argument('--local_only',   action='store_true',
                        help='Skip GCS upload — only produce local clean CSV')
    args = parser.parse_args()

    print("=" * 60)
    print("  Reviews CSV Cleaner")
    print("=" * 60)

    # ── Check input exists
    if not os.path.exists(args.input_csv):
        print(f"\nERROR: Input file not found: {args.input_csv}")
        print("Set --input_csv to the path of olist_order_reviews_dataset.csv")
        sys.exit(1)

    # ── Clean
    stats = clean_reviews_csv(args.input_csv, args.output_csv)

    print()
    print("Results:")
    print(f"  Total output rows:  {stats['total_output']:,}")
    print(f"  Already clean:      {stats['already_good']:,}")
    print(f"  Reconstructed:      {stats['reconstructed']:,}")
    print(f"  Skipped (bad):      {stats['skipped']:,}")
    print(f"  Recovery rate:      {stats['recovery_rate']}%")

    # ── Upload to GCS
    if not args.local_only:
        if not args.bucket:
            print("\nERROR: --bucket required unless --local_only is set")
            sys.exit(1)
        print()
        print("Uploading to GCS...")
        upload_to_gcs(args.output_csv, args.bucket, args.gcs_dest)
        print(f"\nDone. Re-run bq load for reviews using the cleaned file.")
    else:
        print(f"\nDone. Clean file written to: {args.output_csv}")
        print("Upload it manually or re-run without --local_only")


if __name__ == '__main__':
    main()
