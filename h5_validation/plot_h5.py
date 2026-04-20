#!/usr/bin/env python3
import argparse
from pathlib import Path

import numpy as np
import h5py

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DATASET = "Jets"

FIELD_PT = "pt"
FIELD_MASS = "mass"
CAT_FIELDS = ["hadron_flavour", "top_category"]  # optional


def configure_matplotlib():
    plt.rcParams.update({
        "figure.figsize": (11, 7),
        "figure.dpi": 120,
        "savefig.dpi": 200,
        "font.size": 14,
        "axes.titlesize": 20,
        "axes.labelsize": 18,
        "legend.fontsize": 12,
        "xtick.labelsize": 14,
        "ytick.labelsize": 14,
        "axes.linewidth": 1.0,
    })


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


def infer_process(fpath: Path) -> str:
    s = fpath.stem
    if s.startswith("QCD"):
        return "QCD"
    if s.startswith("TT"):
        return "TT"
    if s.startswith("Top"):
        return "Top"
    if s.startswith("WJets"):
        return "WJets"
    if s.startswith("ZJets"):
        return "ZJets"
    if s.startswith("SVJ"):
        return "SVJ"
    if s.startswith("EMJ"):
        return "EMJ"
    if s.startswith("Hbb"):
        return "Hbb"
    if s.startswith("Yto"):
        return "Yto4q"
    return s.split("_")[0]


def group_by_process(files):
    groups = {}
    for f in files:
        groups.setdefault(infer_process(f), []).append(f)
    # stable order
    return dict(sorted(groups.items(), key=lambda kv: kv[0]))


def pretty_axes(field, logy, range_, ymin_log, title, legend_outside=False):
    if field == FIELD_PT:
        plt.xlabel(r"Jet $p_T$ [GeV]")
    elif field == FIELD_MASS:
        plt.xlabel("Jet mass [GeV]")
    else:
        plt.xlabel(field)

    plt.ylabel("Density" if plt.gca().get_legend_handles_labels()[0] else "Density")

    if logy:
        plt.yscale("log")
        plt.ylim(bottom=ymin_log)

    if range_ is not None:
        plt.xlim(range_[0], range_[1])

    if title:
        plt.title(title)

    plt.grid(True, which="both", linestyle="--", linewidth=0.8, alpha=0.5)

    if legend_outside:
        leg = plt.legend(
            loc="upper left",
            bbox_to_anchor=(1.02, 1.0),
            borderaxespad=0.0,
            frameon=True,
            fancybox=True,
            framealpha=0.9,
        )
    else:
        leg = plt.legend(frameon=True, fancybox=True, framealpha=0.9)

    if leg is not None:
        leg.get_frame().set_linewidth(0.8)


def label_for_category(cat_field: str, cat_value: int) -> str:
    if cat_field == "hadron_flavour":
        mapping = {0: "light (0)", 4: "c (4)", 5: "b (5)", -1: "undef (-1)"}
        return mapping.get(int(cat_value), f"{int(cat_value)}")
    if cat_field == "top_category":
        return f"{int(cat_value)}"
    return f"{int(cat_value)}"


def overlay_hist(files, field, outdir, bins, logy, density, range_, max_rows, ymin_log,
                 title=None, outname=None, legend_outside=False):
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
            linewidth=2.2,
            density=density,
            label=fpath.stem,
        )

    if title is None:
        title = f"{DATASET}: {field} overlay"

    pretty_axes(field, logy, range_, ymin_log, title, legend_outside=legend_outside)

    outdir.mkdir(parents=True, exist_ok=True)
    if outname is None:
        outname = f"{field}_overlay.png"
    outpath = outdir / outname

    if legend_outside:
        plt.tight_layout(rect=(0, 0, 0.78, 1))  # reserve space for legend on the right
    else:
        plt.tight_layout()

    plt.savefig(outpath)
    plt.close()
    return outpath


def split_hist(h5_path, field, cat_field, outdir, bins, logy, density, range_, max_rows, ymin_log,
               legend_outside=False):
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
    if len(cats) <= 1:
        return None

    plt.figure()
    cats = np.array(sorted([int(v) for v in cats]))
    for cat in cats:
        sel = (c == cat)
        n_sel = int(np.sum(sel))
        if n_sel == 0:
            continue
        plt.hist(
            x[sel],
            bins=bins,
            range=range_,
            histtype="step",
            linewidth=2.2,
            density=density,
            label=f"{cat_field}={label_for_category(cat_field, cat)} (n={n_sel})",
        )

    title = f"{h5_path.stem}: {field} by {cat_field}"
    pretty_axes(field, logy, range_, ymin_log, title, legend_outside=legend_outside)

    outdir.mkdir(parents=True, exist_ok=True)
    outpath = outdir / f"{h5_path.stem}_{field}_by_{cat_field}.png"

    if legend_outside:
        plt.tight_layout(rect=(0, 0, 0.78, 1))
    else:
        plt.tight_layout()

    plt.savefig(outpath)
    plt.close()
    return outpath


def main():
    configure_matplotlib()

    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", type=Path, help="Directory containing .h5 files")
    ap.add_argument("--files", type=Path, nargs="*", help="Explicit .h5 files")
    ap.add_argument("--out", type=Path, default=Path("h5_validation/out"))
    ap.add_argument("--bins", type=int, default=100)
    ap.add_argument("--logy", action="store_true")
    ap.add_argument("--counts", action="store_true", help="Plot counts instead of density")
    ap.add_argument("--max-rows", type=int, default=None, help="Optional: only read first N jets per file")
    ap.add_argument("--pt-range", type=float, nargs=2, default=[0.0, 600.0])
    ap.add_argument("--mass-range", type=float, nargs=2, default=[0.0, 400.0])
    ap.add_argument("--ymin-log", type=float, default=1e-5, help="Lower y-limit when using --logy")
    ap.add_argument("--overlay-by-process", action="store_true",
                    help="Make separate overlay plots per process (recommended).")
    ap.add_argument("--legend-outside", action="store_true",
                    help="Place legend outside the plot area (recommended).")
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

    for fpath in files:
        f, ds = get_dataset(fpath)
        try:
            print(f"{fpath} : nJets={ds.shape[0]} fields={ds.dtype.names}")
        finally:
            f.close()

    # Overlays: either one big overlay (can be unreadable) or per-process overlays (recommended).
    if args.overlay_by_process:
        groups = group_by_process(files)
        for proc, proc_files in groups.items():
            pt_out = overlay_hist(
                proc_files, FIELD_PT, args.out, args.bins, args.logy, density,
                tuple(args.pt_range), args.max_rows, args.ymin_log,
                title=f"{proc}: Jet $p_T$ overlay",
                outname=f"{proc}_pt_overlay.png",
                legend_outside=args.legend_outside,
            )
            ms_out = overlay_hist(
                proc_files, FIELD_MASS, args.out, args.bins, args.logy, density,
                tuple(args.mass_range), args.max_rows, args.ymin_log,
                title=f"{proc}: Jet mass overlay",
                outname=f"{proc}_mass_overlay.png",
                legend_outside=args.legend_outside,
            )
            print("Saved:", pt_out)
            print("Saved:", ms_out)
    else:
        pt_out = overlay_hist(
            files, FIELD_PT, args.out, args.bins, args.logy, density,
            tuple(args.pt_range), args.max_rows, args.ymin_log,
            title=f"{DATASET}: Jet $p_T$ overlay",
            outname="pt_overlay.png",
            legend_outside=args.legend_outside,
        )
        ms_out = overlay_hist(
            files, FIELD_MASS, args.out, args.bins, args.logy, density,
            tuple(args.mass_range), args.max_rows, args.ymin_log,
            title=f"{DATASET}: Jet mass overlay",
            outname="mass_overlay.png",
            legend_outside=args.legend_outside,
        )
        print("Saved:", pt_out)
        print("Saved:", ms_out)

    # Bonus: split plots if categories exist and are non-trivial
    # Restrict hadron_flavour splits to QCD only
    for fpath in files:
        is_qcd = fpath.name.startswith("QCD")
        for cat in CAT_FIELDS:
            if cat == "hadron_flavour" and not is_qcd:
                continue

            out = split_hist(
                fpath, FIELD_PT, cat, args.out, args.bins, args.logy, density,
                tuple(args.pt_range), args.max_rows, args.ymin_log,
                legend_outside=args.legend_outside,
            )
            if out:
                print("Saved:", out)

            out = split_hist(
                fpath, FIELD_MASS, cat, args.out, args.bins, args.logy, density,
                tuple(args.mass_range), args.max_rows, args.ymin_log,
                legend_outside=args.legend_outside,
            )
            if out:
                print("Saved:", out)


if __name__ == "__main__":
    main()