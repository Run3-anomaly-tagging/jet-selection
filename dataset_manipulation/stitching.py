import os
import json
import h5py
import numpy as np
import argparse

PT_MIN, PT_MAX, PT_BIN_WIDTH = 170, 610, 20

def get_dataset_entries(config_path, dataset_prefix, output_dir):
    """Return list of dataset entries matching prefix from config with h5 file path, xsec, etc."""
    with open(config_path, "r") as f:
        cfg = json.load(f)
    entries = []
    for ds in cfg["datasets"]:
        if ds["name"].startswith(dataset_prefix):
            h5_path = os.path.join(output_dir, f"{ds['name']}.h5")
            entries.append({
                "name": ds["name"],
                "h5_path": h5_path,
                "xsec_pb": ds["xsec_pb"],
            })
    return entries

def get_jet_pts(h5_path):
    """Load jet pt array from h5 file."""
    with h5py.File(h5_path, "r") as f:
        pts = f["Jets"]["pt"][:]
    return pts

def get_all_columns(h5_path):
    """Return all columns from the first file (assume all files have same columns)."""
    with h5py.File(h5_path, "r") as f:
        cols = list(f["Jets"].dtype.fields.keys())
    return cols

def get_jet_data(h5_path, cols):
    """Return dict of {col: array} for all jets in file."""
    with h5py.File(h5_path, "r") as f:
        jets = f["Jets"][:]
        data = {col: jets[col] for col in cols}
    return data

def bin_jet_pts(pts, pt_edges):
    """Return array of bin indices for each pt."""
    return np.digitize(pts, pt_edges) - 1  # bins start at 0

def report_counts_and_lumi(entries, pt_edges=None):
    """Print effective lumi per file and total jet count per pT bin (if pt_edges provided)."""
    print("Effective lumi per file:")
    header = "File".ljust(20) + "Total".rjust(8) + " xsec(pb)".rjust(12) + " eff.lumi(pb^-1)".rjust(18)
    print(header)
    all_counts = []
    eff_lumis = []
    for entry in entries:
        pts = get_jet_pts(entry["h5_path"])
        total = len(pts)
        xsec = entry["xsec_pb"]
        lumi = total / xsec if xsec > 0 else 0
        eff_lumis.append(lumi)
        row = entry["name"].ljust(20) + f"{total:8d}{xsec:12.2f}{lumi:18.3f}"
        print(row)
        
        if pt_edges is not None:
            bins = bin_jet_pts(pts, pt_edges)
            counts = [(bins == i).sum() for i in range(len(pt_edges)-1)]
            all_counts.append(counts)
    
    result = {"eff_lumis": np.array(eff_lumis)}
    
    if pt_edges is not None:
        all_counts = np.array(all_counts)
        total_per_bin = np.sum(all_counts, axis=0)
        print("\nTotal jet count per pT bin across all files:")
        for i in range(len(pt_edges)-1):
            pt_range = f"[{pt_edges[i]},{pt_edges[i+1]})"
            print(f"{pt_range}: {total_per_bin[i]}")
        min_total_bin = np.min(total_per_bin)
        print(f"\nFor flat merge: will take {min_total_bin} jets per pT bin (lowest bin count).")
        result["total_per_bin"] = total_per_bin
        result["min_total_bin"] = min_total_bin
    
    return result

def downsample_indices(indices, n):
    """Randomly select n indices from indices array."""
    if len(indices) <= n:
        return indices
    return np.random.choice(indices, n, replace=False)

def merge_realistic(entries, cols, eff_lumis, out_path):
    """Downsample each file to match the minimum effective lumi."""
    merged = {col: [] for col in cols}
    lumi_min = np.min(eff_lumis)
    for entry, lumi in zip(entries, eff_lumis):
        data = get_jet_data(entry["h5_path"], cols)
        xsec = entry["xsec_pb"]
        n_keep = int(lumi_min * xsec)
        n_avail = len(data[cols[0]])
        n = min(n_keep, n_avail)
        idx = downsample_indices(np.arange(n_avail), n)
        for col in cols:
            merged[col].append(data[col][idx])
        print(f"{entry['name']}: keeping {n} jets (xsec={xsec}, lumi_min={lumi_min:.3f} pb^-1)")
    # Concatenate and shuffle before saving
    # It slows down execution time, but ensures jets from different files are mixed together in the outputs
    # If memory becomes an issue, this should be rewritten in batches
    with h5py.File(out_path, "w") as fout:
        g = fout.create_group("Events")
        arrs = {col: np.concatenate(merged[col]) for col in cols}
        n_total = len(arrs[cols[0]])
        shuffle_idx = np.random.permutation(n_total)
        for col in cols:
            g.create_dataset(col, data=arrs[col][shuffle_idx], compression="gzip")
    print(f"Saved {out_path} with {sum([len(x) for x in merged[cols[0]]])} jets.")

def merge_flat(entries, cols, pt_edges, total_per_bin, min_total_bin, out_path):
    """Downsample in each pT bin to min_total_bin, then merge from all files."""
    # Collect jets from all files per bin
    jets_per_bin = {i: {col: [] for col in cols} for i in range(len(pt_edges)-1)}
    for entry in entries:
        data = get_jet_data(entry["h5_path"], cols)
        pts = data["pt"]
        bins = bin_jet_pts(pts, pt_edges)
        for i in range(len(pt_edges)-1):
            idx_in_bin = np.where(bins == i)[0]
            for col in cols:
                jets_per_bin[i][col].append(data[col][idx_in_bin])
    # For each bin, concatenate jets from all files, downsample to min_total_bin, and collect
    merged = {col: [] for col in cols}
    for i in range(len(pt_edges)-1):
        bin_data = {col: np.concatenate(jets_per_bin[i][col]) if jets_per_bin[i][col] else np.array([]) for col in cols}
        n_avail = len(bin_data[cols[0]])
        n = min(min_total_bin, n_avail)
        if n > 0:
            idx = downsample_indices(np.arange(n_avail), n)
            for col in cols:
                merged[col].append(bin_data[col][idx])
    # Concatenate and shuffle before saving
    with h5py.File(out_path, "w") as fout:
        g = fout.create_group("Events")
        if merged[cols[0]]:
            arrs = {col: np.concatenate(merged[col]) for col in cols}
            n_total = len(arrs[cols[0]])
            shuffle_idx = np.random.permutation(n_total)
            for col in cols:
                g.create_dataset(col, data=arrs[col][shuffle_idx], compression="gzip")
        else:
            for col in cols:
                g.create_dataset(col, data=np.array([]), compression="gzip")
    print(f"Saved {out_path} with {sum([len(x) for x in merged[cols[0]]])} jets.")

def main():
    parser = argparse.ArgumentParser(description="Stitch HDF5 files with optional flat or realistic pT spectrum.")
    parser.add_argument("--dataset-prefix", default="QCD", help="Dataset prefix to match (e.g., 'QCD', 'ZJets')")
    parser.add_argument("--merge-type", choices=["flat", "realistic", "both"], default="both", 
                        help="Merge type: flat (pT bins), realistic (lumi matching), or both")
    parser.add_argument("--output-dir", default="../data", help="Output directory for merged files")
    parser.add_argument("--config", default="../condor_submission/config.json", help="Path to config.json")
    args = parser.parse_args()
    
    np.random.seed(1)
    
    entries = get_dataset_entries(args.config, args.dataset_prefix, args.output_dir)
    if not entries:
        print(f"No datasets found matching prefix '{args.dataset_prefix}' in config.")
        return
    
    pt_edges = None
    if args.merge_type in ["flat", "both"]:
        pt_edges = np.arange(PT_MIN, PT_MAX+PT_BIN_WIDTH, PT_BIN_WIDTH)
    
    report_result = report_counts_and_lumi(entries, pt_edges)
    eff_lumis = report_result["eff_lumis"]
    lumi_min = np.min(eff_lumis)
    
    if args.merge_type in ["realistic", "both"]:
        print(f"\nFor realistic merge: will downsample all files to lumi = {lumi_min:.3f} pb^-1")
    
    proceed = input("\nProceed with merging? [y/N]: ")
    if proceed.strip().lower() != "y":
        print("Aborted.")
        return
    
    cols = get_all_columns(entries[0]["h5_path"])
    
    output_realistic = os.path.join(args.output_dir, f"{args.dataset_prefix}_merged_realistic.h5")
    output_flat = os.path.join(args.output_dir, f"{args.dataset_prefix}_merged_flat.h5")
    
    # Perform merging
    if args.merge_type in ["realistic", "both"]:
        merge_realistic(entries, cols, eff_lumis, output_realistic)
    
    if args.merge_type in ["flat", "both"]:
        merge_flat(entries, cols, pt_edges, report_result["total_per_bin"], 
                   report_result["min_total_bin"], output_flat)

if __name__ == "__main__":
    main()
