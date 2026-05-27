#!/usr/bin/env python3
"""
cb-sb_match_sub.py — tags pipeline launcher.

For each row in a file_paths.csv where run == TRUE, submit a cb-sb_match sbatch job.
Output and log locations are caller-supplied so the pipeline is reusable across analyses.

Usage:
    python cb-sb_match_sub.py --odir-base /path/to/outputs/restored \
                              [--csv config/file_paths.csv] \
                              [--log-dir /path/to/logs] [--dry-run]

Per-sample matcher output lands in <odir-base>/<spl>/df_whitelist.txt.
SLURM logs (<log-dir>/%j_cb-sb_match.{out,err}) override the sbatch wrapper defaults.
"""

import argparse
import csv
import json
import os
import shutil
import subprocess
from datetime import datetime

PIPE_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV = os.path.join(PIPE_DIR, "config", "file_paths.csv")
SBATCH_SCRIPT = os.path.join(PIPE_DIR, "cb-sb_match_sbatch.sh")


def pipeline_version():
    """Git commit + dirty flag of the pipeline checkout, for output provenance."""
    def git(*a):
        return subprocess.check_output(
            ["git", "-C", PIPE_DIR, *a], text=True, stderr=subprocess.DEVNULL
        ).strip()
    try:
        commit = git("rev-parse", "HEAD")
        dirty = bool(git("status", "--porcelain"))
    except Exception:
        return {"commit": "unknown", "dirty": None}
    return {"commit": commit, "dirty": dirty}


def running_job_names():
    """Set of job names currently in this user's SLURM queue."""
    try:
        out = subprocess.check_output(
            ["squeue", "-u", os.environ.get("USER", "beo703"),
             "--format=%j", "--noheader"],
            text=True,
        )
        return set(out.split())
    except Exception:
        return set()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("-odir", "--odir", "--odir-base", dest="odir_base", required=True,
                   help="base dir for per-sample output (<odir>/<spl>/)")
    p.add_argument("--csv", default=DEFAULT_CSV,
                   help=f"sample table (default: {DEFAULT_CSV})")
    p.add_argument("--log-dir", default=None,
                   help="dir for SLURM logs (default: <odir-base>/../logs)")
    p.add_argument("--dry-run", action="store_true",
                   help="print sbatch commands without submitting")
    args = p.parse_args()

    # Each invocation gets its own timestamped run dir under the supplied base,
    # so re-runs never clobber prior output: <odir>/<YYMMDD_HHMMSS>/<spl>/.
    stamp = datetime.now().strftime("%y%m%d_%H%M%S")
    odir_base = os.path.join(os.path.abspath(args.odir_base), stamp)
    log_dir = os.path.abspath(args.log_dir) if args.log_dir \
        else os.path.join(odir_base, "logs")
    ver = pipeline_version()
    print(f"run dir: {odir_base}")
    print(f"pipeline commit: {ver['commit'][:12]}"
          f"{' (dirty)' if ver['dirty'] else ''}")
    if not args.dry_run:
        os.makedirs(odir_base, exist_ok=True)
        os.makedirs(log_dir, exist_ok=True)
        # Provenance: pin each run dir to the pipeline version + inputs that made it.
        with open(os.path.join(odir_base, "pipeline_version.json"), "w") as fh:
            json.dump({
                "timestamp": stamp,
                "pipeline_dir": PIPE_DIR,
                "git_commit": ver["commit"],
                "git_dirty": ver["dirty"],
                "csv": os.path.abspath(args.csv),
            }, fh, indent=2)
        shutil.copy(args.csv, os.path.join(odir_base, "file_paths.used.csv"))

    queued = running_job_names()

    with open(args.csv) as fh:
        reader = csv.DictReader(fh, skipinitialspace=True)
        for row in reader:
            if row.get("run", "").strip() != "TRUE":
                continue
            spl      = row["spl"].strip()
            r1       = row["r1_fq"].strip().strip('"')
            r2       = row["r2_fq"].strip().strip('"')
            wl       = row["wl"].strip().strip('"')
            cb_fmats = row.get("cb_fmats", "").strip().strip('"')
            if not wl:
                print(f"  SKIP {spl}: no whitelist")
                continue
            odir = os.path.join(odir_base, spl)
            if os.path.exists(os.path.join(odir, "df_whitelist.txt")):
                print(f"  SKIP {spl}: output already exists")
                continue
            if f"cbsb_{spl}" in queued:
                print(f"  SKIP {spl}: already queued")
                continue
            cmd = ["sbatch", f"--job-name=cbsb_{spl}",
                   "-o", os.path.join(log_dir, "%j_cb-sb_match.out"),
                   "-e", os.path.join(log_dir, "%j_cb-sb_match.err"),
                   SBATCH_SCRIPT, r1, r2, wl, odir]
            if cb_fmats:
                cmd += ["--cb-fmats", cb_fmats]
            print(" ".join(cmd))
            if not args.dry_run:
                subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
