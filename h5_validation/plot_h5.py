#!/usr/bin/env python3
import argparse
from pathlib import Path

import numpy as np
import h5py

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATASET = "Jets"

# Your observed field names:
FIELD_PT = "pt"
FIELD_MASS = "mass"
CAT_FIELDS = ["hadron_flavour", "top_category"]  # optional


def iter_h5_files(dir_path: Path):
    return sorted([p for p in dir_path.glob("*.h5") if p.is_file()])


def get_dataset(h5_path: Path):
    f = h5py.File(h5_path, "r")
    if DATASET not in f:
        f.close()
        raise KeyError(f"{h5_path}: missing dataset '{DATASET}'")
    return f, f[DATASET]


def check_fields(ds, needed):
    names = ds.dtype.names or ()
    missing = [k for k in needed if k not in names]
    if missing:
        raise KeyError(f"Missing fields {missing}. Available fields: {names}")


def load_field(ds, field, max_rows=None):
    n = ds.shape[0]
    if max_rows is None or max_rows >= n:
        return ds[field][...]
    return ds[field][:max_rows]


def validate_numeric(x, name, h5_path):
    if x.size == 0:
        raise ValueError(f"{h5_path}: '{name}' is empty")
    if not np.isfinite(x).all():
        bad = np.sum(~np.isfinite(x))
        raise ValueError(f"{h5_path}: '{name}' has {bad} non-finite entries")
    if name in (FIELD_PT, FIELD_MASS) and (x < 0).any():
        neg = np.sum(x < 0)
        raise ValueError(f"{h5_path}: '{name}' has {neg} negative entries")


def overlay_hist(files, field, outdir, bins, logy, density, range_, max_rows):
    plt.figure()
    for fpath in files:
        f, ds = get_dataset(fpath)
        try:
            check_fields(ds, [field])
            x = load_field(ds, field, max_rows=max_rows)
        finally:
            f.close()

        validate_numeric(x, field, fpath)
        plt.hist(
            x,
            bins=bins,
            range=range_,
            histtype="step",
            linewidth=1.5,
            density=density,
            label=fpath.name,
        )

    plt.xlabel(field)
    plt.ylabel("density" if density else "count")
    if logy:
        plt.yscale("log")
    plt.legend(fontsize=8)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{field}_overlay.png"
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    return outpath


def split_hist(h5_path, field, cat_field, outdir, bins, logy, density, range_, max_rows):
    f, ds = get_dataset(h5_path)
    try:
        names = ds.dtype.names or ()
        if field not in names or cat_field not in names:
            return None

        x = load_field(ds, field, max_rows=max_rows)
        c = load_field(ds, cat_field, max_rows=max_rows)
    finally:
        f.close()

    validate_numeric(x, field, h5_path)

    cats = np.unique(c)
    # If only one category, splitting is not useful
    if len(cats) <= 1:
        return None

    plt.figure()
    for cat in cats:
        sel = (c == cat)
        if np.sum(sel) == 0:
            continue
        plt.hist(
            x[sel],
            bins=bins,
            range=range_,
            histtype="step",
            linewidth=1.5,
            density=density,
            label=f"{cat_field}={cat} (n={np.sum(sel)})",
        )

    plt.xlabel(field)
    plt.ylabel("density" if density else "count")
    if logy:
        plt.yscale("log")
    plt.legend(fontsize=7)
    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{h5_path.stem}_{field}_by_{cat_field}.png"
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()
    return outpath


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, help="Directory containing .h5 files")
    ap.add_argument("--files", type=Path, nargs="*", help="Explicit .h5 files")
    ap.add_argument("--out", type=Path, default=Path("h5_validation/out"))
    ap.add_argument("--bins", type=int, default=100)
    ap.add_argument("--logy", action="store_true")
    ap.add_argument("--counts", action="store_true", help="Plot counts instead of density")
    ap.add_argument("--max-rows", type=int, default=None, help="Optional: only read first N jets per file")
    ap.add_argument("--pt-range", type=float, nargs=2, default=[0.0, 2500.0])
    ap.add_argument("--mass-range", type=float, nargs=2, default=[0.0, 500.0])
    args = ap.parse_args()

    if args.files:
        files = list(args.files)
    elif args.dir:
        files = iter_h5_files(args.dir)
    else:
        raise SystemExit("Provide --dir or --files")

    if not files:
        raise SystemExit("No .h5 files found")

    density = not args.counts

    # Print schema summary for each file (fast: does not load full arrays)
    for fpath in files:
        f, ds = get_dataset(fpath)
        try:
            print(f"{fpath} : nJets={ds.shape[0]} fields={ds.dtype.names}")
        finally:
            f.close()

    pt_out = overlay_hist(files, FIELD_PT, args.out, args.bins, args.logy, density, tuple(args.pt_range), args.max_rows)
    ms_out = overlay_hist(files, FIELD_MASS, args.out, args.bins, args.logy, density, tuple(args.mass_range), args.max_rows)
    print("Saved:", pt_out)
    print("Saved:", ms_out)

    # Bonus: split plots if categories exist and are non-trivial
    for fpath in files:
        for cat in CAT_FIELDS:
            out = split_hist(fpath, FIELD_PT, cat, args.out, args.bins, args.logy, density, tuple(args.pt_range), args.max_rows)
            if out:
                print("Saved:", out)
            out = split_hist(fpath, FIELD_MASS, cat, args.out, args.bins, args.logy, density, tuple(args.mass_range), args.max_rows)
            if out:
                print("Saved:", out)


if __name__ == "__main__":
    main()