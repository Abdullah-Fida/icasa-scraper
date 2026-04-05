"""
Cross-platform pipeline runner for GitHub Actions (and local use).
Replaces run_full_pipeline.bat for Linux/macOS/Windows compatibility.

Usage:
    python run_pipeline.py
"""

import subprocess
import sys
import os
import shutil
from datetime import datetime


def run_step(description, cmd):
    """Run a pipeline step and abort on failure."""
    print(f"\n{'='*60}")
    print(f"  {description}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, shell=False)
    if result.returncode != 0:
        print(f"\n[ERROR] {description} failed with exit code {result.returncode}")
        sys.exit(result.returncode)
    print(f"[OK] {description} completed successfully.")


def main():
    print("Starting Real Estate Scraper Pipeline (Phases 1-5, 8, 9)")
    print("=" * 60)

    out_dir = "output"

    # --- 1. Backup old output (if exists) ---
    if os.path.isdir(out_dir):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = os.path.join("backups", f"output_backup_{timestamp}")
        os.makedirs(backup_dir, exist_ok=True)
        print(f"Backing up output/ -> {backup_dir}")
        shutil.copytree(out_dir, backup_dir, dirs_exist_ok=True)

        # Clear current output
        for f in os.listdir(out_dir):
            fp = os.path.join(out_dir, f)
            if os.path.isfile(fp):
                os.remove(fp)
    else:
        os.makedirs(out_dir, exist_ok=True)

    py = sys.executable  # Use the same Python that is running this script

    # --- 2. Execute pipeline phases ---
    run_step(
        "Phase 1: Scraping URLs",
        [py, "scraper_icasa.py", "--urls-only"]
    )

    run_step(
        "Phase 2: Checking URLs against API",
        [py, "check_urls.py",
         "--buy", "output/buy_urls.txt",
         "--rent", "output/rent_urls.txt",
         "--out", "output/results.jsonl",
         "--json", "output/result.json",
         "--workers", "10",
         "--delay", "0.0"]
    )

    run_step(
        "Phase 3: Scraping Missing Detail Pages",
        [py, "phase3_scrape.py", "--workers", "25", "--delay", "0.0"]
    )

    run_step(
        "Phase 4: Cleaning Agencies",
        [py, "phase4_clean.py"]
    )

    run_step(
        "Phase 5: Executing API Contact Checks",
        [py, "phase5_api.py"]
    )

    run_step(
        "Phase 8: Applying External IDs and Categories",
        [py, "phase8_process.py"]
    )

    run_step(
        "Phase 9: Final Formatting and Data Cleaning",
        [py, "phase9_process.py"]
    )

    print(f"\n{'='*60}")
    print("Pipeline completed successfully!")
    print("Final results are in the output/ directory (Phase9_*.csv).")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
