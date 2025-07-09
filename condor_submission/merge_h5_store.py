#!/usr/bin/env python3
"""
merge_h5_store.py

Merge HDF5 files stored on FNAL EOS into per-sample .h5 files, preserving group structure.

For each sample folder under STORE_INPUT_BASE:
  1) List .h5 files via `eos root://cmseos.fnal.gov/ ls`
  2) Copy each file locally with `xrdcp`
  3) On the first file, scan all dataset paths and create matching resizable datasets in the merged file
  4) For each file, append every dataset to the merged one
  5) Clean up local chunk files and upload the merged file back to EOS

Usage:
  1. Ensure STORE_OUTPUT_DIR exists on EOS:
       eos root://cmseos.fnal.gov/ mkdir -p /store/user/ecakir/anomaly-tagging/merged_H5
  2. Install h5py: `python3 -m pip install --user h5py`
  3. Run:
       chmod +x merge_h5_store.py
       ./merge_h5_store.py
"""
import os
import subprocess
import h5py

STORE_INPUT_BASE = "/store/user/ecakir/anomaly-tagging"
STORE_OUTPUT_DIR = "/store/user/ecakir/anomaly-tagging/merged_H5"
LOCAL_WORKDIR    = "/tmp/h5_merge_work"

EOS_CMD    = "eos"
XRDCP_CMD  = "xrdcp"
EOS_PREFIX = "root://cmseos.fnal.gov/"
XRD_PREFIX = "root://cmseos.fnal.gov/"

def eos_ls(path):
    """List entries in an EOS directory."""
    cmd = [EOS_CMD, EOS_PREFIX, "ls", path]
    out = subprocess.check_output(cmd)
    return out.decode().split()


def xrdcp_in(remote, local):
    """Copy from EOS → local disk."""
    cmd = [XRDCP_CMD, "-f", f"{XRD_PREFIX}{remote}", local]
    subprocess.check_call(cmd)


def xrdcp_out(local, remote):
    """Copy from local disk → EOS."""
    cmd = [XRDCP_CMD, "-f", local, f"{XRD_PREFIX}{remote}"]
    subprocess.check_call(cmd)

os.makedirs(LOCAL_WORKDIR, exist_ok=True)

# Pre-cache existing merged outputs
try:
    existing = set(eos_ls(STORE_OUTPUT_DIR))
except subprocess.CalledProcessError:
    existing = set()

for sample in eos_ls(STORE_INPUT_BASE):
    sample_dir = f"{STORE_INPUT_BASE}/{sample}"
    # gather H5 files in this folder
    try:
        files = [f for f in eos_ls(sample_dir) if f.endswith('.h5')]
    except subprocess.CalledProcessError:
        continue
    if not files:
        continue

    merged_name   = f"{sample}.h5"
    local_merged  = os.path.join(LOCAL_WORKDIR, merged_name)
    remote_merged = f"{STORE_OUTPUT_DIR}/{merged_name}"

    # ask before overwriting an existing merged file
    if merged_name in existing:
        ans = input(f"Merged file '{merged_name}' already exists in {STORE_OUTPUT_DIR}. Overwrite? [y/N]: ")
        if ans.strip().lower() != 'y':
            print(f"[skip] {merged_name} not overwritten")
            continue
        else:
            print(f"[recreate] Overwriting existing merged file for {sample}")

    # remove stale local merge if any
    if os.path.exists(local_merged):
        os.remove(local_merged)

    print(f"[start] {sample}: merging {len(files)} files")
    dataset_paths = []

    for idx, fname in enumerate(files):
        remote_file = f"{sample_dir}/{fname}"
        local_file  = os.path.join(LOCAL_WORKDIR, fname)

        print(f"[download] {remote_file}")
        xrdcp_in(remote_file, local_file)

    print(f"[start] {sample}: merging {len(files)} files")
    dataset_paths = []

    for idx, fname in enumerate(files):
        remote_file = f"{sample_dir}/{fname}"
        local_file  = os.path.join(LOCAL_WORKDIR, fname)

        print(f"[download] {remote_file}")
        xrdcp_in(remote_file, local_file)

        # On first file, collect all dataset paths and initialize merged file
        if idx == 0:
            with h5py.File(local_file, 'r') as f0:
                # traverse to collect dataset paths
                f0.visititems(lambda path, obj: dataset_paths.append(path) if isinstance(obj, h5py.Dataset) else None)
                # create merged file structure
                with h5py.File(local_merged, 'w') as fout:
                    for path in dataset_paths:
                        ds0 = f0[path]
                        # ensure parent groups exist
                        parent = os.path.dirname(path)
                        if parent:
                            fout.require_group(parent)
                        # create resizable dataset
                        shape_tail = ds0.shape[1:]
                        fout.create_dataset(
                            path,
                            shape=(0, *shape_tail),
                            maxshape=(None, *shape_tail),
                            chunks=True,
                            dtype=ds0.dtype
                        )

        # append data for each dataset
        with h5py.File(local_file, 'r') as fin, h5py.File(local_merged, 'a') as fout:
            # verify same schema
            cur_paths = []
            fin.visititems(lambda path, obj: cur_paths.append(path) if isinstance(obj, h5py.Dataset) else None)
            if set(cur_paths) != set(dataset_paths):
                raise RuntimeError(f"Schema mismatch in {local_file}")
            for path in dataset_paths:
                data = fin[path][...]
                ds = fout[path]
                old = ds.shape[0]
                ds.resize((old + data.shape[0], *data.shape[1:]))
                ds[old:] = data

        os.remove(local_file)

    print(f"[upload] {local_merged} → {remote_merged}")
    xrdcp_out(local_merged, remote_merged)
    os.remove(local_merged)
    print(f"[done] {sample}\n")
