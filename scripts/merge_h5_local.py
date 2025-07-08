#!/usr/bin/env python3
"""
merge_h5_local_complete.py

For each subdirectory under ~/nobackup/merging_files:
  - finds all .h5 files
  - checks that they have the same full set of dataset paths (including groups)
  - creates one merged .h5 in merged_H5/, with resizable datasets mirroring the original hierarchy
  - appends each file’s data in turn

Usage:
  chmod +x merge_h5_local_complete.py
  ./merge_h5_local_complete.py
"""

import os
import h5py

# ─── CONFIG ─────────────────────────────────────────────────────────────────────
INPUT_BASE = os.path.expanduser("~/nobackup/merging_files")
OUTPUT_DIR = os.path.join(INPUT_BASE, "merged_H5")

# ─── HELPERS ────────────────────────────────────────────────────────────────────
def fs_ls(path):
    """List non-hidden entries in a local filesystem directory."""
    return [e for e in os.listdir(path) if not e.startswith(".")]

def collect_dataset_paths(h5file):
    """
    Return a list of all dataset paths in this open HDF5 file.
    Example: ["group1/data", "group2/sub/data2", "plain_dataset"]
    """
    paths = []
    def visitor(name, obj):
        if isinstance(obj, h5py.Dataset):
            paths.append(name)
    h5file.visititems(visitor)
    return paths

# ─── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for sample in fs_ls(INPUT_BASE):
        sample_in = os.path.join(INPUT_BASE, sample)
        # skip non-directories
        if not os.path.isdir(sample_in):
            continue

        sample_out = os.path.join(OUTPUT_DIR, f"{sample}.h5")
        if os.path.exists(sample_out):
            print(f"[skip] already merged: {sample_out}")
            continue

        # gather all .h5 files
        files = [
            os.path.join(sample_in, f)
            for f in fs_ls(sample_in)
            if f.endswith(".h5")
        ]
        if not files:
            print(f"[warn] no .h5 files in {sample_in}, skipping")
            continue

        # (1) collect dataset paths from the first file
        with h5py.File(files[0], "r") as f0:
            dataset_paths = collect_dataset_paths(f0)

        # (2) verify every file has the same dataset paths
        for fn in files[1:]:
            with h5py.File(fn, "r") as f:
                other_paths = collect_dataset_paths(f)
                if set(other_paths) != set(dataset_paths):
                    raise RuntimeError(
                        f"Dataset-path mismatch in {fn}\n"
                        f" got: {sorted(other_paths)}\n"
                        f"need: {sorted(dataset_paths)}"
                    )

        # (3) create the merged file and empty, resizable datasets
        #     also record shapes and dtypes for each path
        shapes = {}
        with h5py.File(sample_out, "w") as fout, h5py.File(files[0], "r") as f0:
            for path in dataset_paths:
                grp_path, ds_name = os.path.split(path)
                grp = fout.require_group(grp_path) if grp_path else fout

                template = f0[path]
                entry_shape = template.shape[1:]
                shapes[path] = entry_shape

                grp.create_dataset(
                    ds_name,
                    shape=(0, *entry_shape),
                    maxshape=(None, *entry_shape),
                    chunks=True,
                    dtype=template.dtype
                )

            # (4) append data from each file
            for fn in files:
                with h5py.File(fn, "r") as fin:
                    for path in dataset_paths:
                        data = fin[path][...]         # load entire dataset
                        ds   = fout[path]             # merged dataset
                        oldn = ds.shape[0]
                        newn = oldn + data.shape[0]
                        ds.resize((newn, *shapes[path]))
                        ds[oldn:newn, ...] = data

        print(f"[done] merged {len(files)} → {sample_out}")

if __name__ == "__main__":
    main()
