"""
Merge individual .h5 files on EOS into one .h5 file per sample.

If 3 arguments are given, the second is treated as a single sample name to process. Otherwise, all subdirectories under the specified EOS path are processed.

  1. Copies all .h5 chunk files locally using `xrdcp`
  2. Creates a new merged .h5 file by appending datasets from all chunks
  3. Cleans up local temporary files
  4. Uploads the merged file back to EOS as: <sample_name>.h5 (in the STORE_DATABASE_PATH directory)

Usage:
    python merge_h5_store.py <eos_path>
"""
import os
import sys
import subprocess
import h5py

# Get path from first argument, or show usage
if len(sys.argv) < 2:
    print(f"Example usage: python merge_h5_store.py /store/user/roguljic/anomaly-tagging/H5_files/2022_postEE [dataset_name]")
    sys.exit(1)

STORE_DATABASE_PATH = sys.argv[1]
SINGLE_SAMPLE = sys.argv[2] if len(sys.argv) > 2 else None
LOCAL_WORKDIR    = "/tmp/h5_merge_work"

EOS_CMD    = "eos"
XRDCP_CMD  = "xrdcp"
RDR_PREFIX = "root://cmseos.fnal.gov/"

def eos_ls(path):
    """List entries in an EOS directory."""
    cmd = [EOS_CMD, RDR_PREFIX, "ls", path]
    out = subprocess.check_output(cmd)
    return out.decode().split()


def xrdcp_in(remote, local):
    """Copy from EOS -> local disk."""
    cmd = [XRDCP_CMD, "-f", f"{RDR_PREFIX}{remote}", local]
    subprocess.check_call(cmd)


def xrdcp_out(local, remote):
    """Copy from local disk -> EOS."""
    cmd = [XRDCP_CMD, "-f", local, f"{RDR_PREFIX}{remote}"]
    subprocess.check_call(cmd)

os.makedirs(LOCAL_WORKDIR, exist_ok=True)

if SINGLE_SAMPLE:
    samples = [SINGLE_SAMPLE]
else:
    samples = [s for s in eos_ls(STORE_DATABASE_PATH) if not s.endswith('.h5')]

for sample in samples:
    sample_dir = f"{STORE_DATABASE_PATH}/{sample}"

    try:
        existing = set(eos_ls(STORE_DATABASE_PATH))
    except subprocess.CalledProcessError:
        existing = set()

    # gather H5 files in this folder
    try:
        files = [f for f in eos_ls(sample_dir) if f.endswith('.h5')]
    except subprocess.CalledProcessError:
        continue
    if not files:
        continue

    merged_name   = f"{sample}.h5"
    local_merged  = os.path.join(LOCAL_WORKDIR, merged_name)
    remote_merged = f"{STORE_DATABASE_PATH}/{merged_name}"

    # ask before overwriting an existing merged file
    if merged_name in existing:
        ans = input(f"Merged file '{merged_name}' already exists in {sample_dir}. Overwrite? [y/N]: ")
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

    print(f"[upload] {local_merged} -> {remote_merged}")
    xrdcp_out(local_merged, remote_merged)
    os.remove(local_merged)
    print(f"[done] {sample}\n")
