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
        jets = f["Jets"]
        if isinstance(jets, h5py.Dataset):
            # Structured array format
            cols = list(jets.dtype.names)
        else:
            # Group format (fallback)
            cols = list(jets.keys())
    return cols

def get_jet_data(h5_path, cols):
    """Return dict of {col: array} for all jets in file."""
    with h5py.File(h5_path, "r") as f:
        jets = f["Jets"]
        if isinstance(jets, h5py.Dataset):
            data = {col: jets[col][:] for col in cols}
        else:
            data = {col: jets[col][:] for col in cols}
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
    """Downsample each file to match the minimum effective lumi, shuffle in memory, then write."""
    lumi_min = np.min(eff_lumis)
    
    first_file_path = entries[0]["h5_path"]
    with h5py.File(first_file_path, "r") as fin:
        dtype = fin["Jets"].dtype
    
    # Load and downsample data from all files into memory
    # Uses a lot of memory, but does not require multiple passes through the files
    all_jets = []
    for entry, lumi in zip(entries, eff_lumis):
        xsec = entry["xsec_pb"]
        n_keep = int(lumi_min * xsec)
        with h5py.File(entry["h5_path"], "r") as f:
            n_avail = len(f["Jets"])
            n = min(n_keep, n_avail)
            jets = f["Jets"][:]
            
        print(f"{entry['name']}: loading {n} jets (xsec={xsec}, lumi_min={lumi_min:.3f} pb^-1)")
        all_jets.append(jets[:n])
    
    combined_jets = np.concatenate(all_jets)
    
    print(f"Shuffling {len(combined_jets)} jets...")
    np.random.shuffle(combined_jets)
    
    with h5py.File(out_path, "w") as fout:
        out_dataset = fout.create_dataset("Jets", data=combined_jets)
    
    print(f"Saved {out_path} with {len(combined_jets)} jets.")

def merge_flat(entries, cols, pt_edges, total_per_bin, min_total_bin, out_path):
    """Downsample in each pT bin to min_total_bin, load in memory, shuffle, then write."""
    first_file_path = entries[0]["h5_path"]
    with h5py.File(first_file_path, "r") as fin:
        dtype = fin["Jets"].dtype
    
    bin_jets = {i: [] for i in range(len(pt_edges)-1)}
    
    for entry in entries:
        with h5py.File(entry["h5_path"], "r") as f:
            all_jets = f["Jets"][:]
            pts = all_jets["pt"]
            bins = bin_jet_pts(pts, pt_edges)
            
            for i in range(len(pt_edges)-1):
                mask = bins == i
                bin_jets[i].extend(all_jets[mask])
    
    all_selected_jets = []
    for i in range(len(pt_edges)-1):
        if not bin_jets[i]:
            continue
        
        bin_array = np.array(bin_jets[i])
        n_avail = len(bin_array)
        n = min(min_total_bin, n_avail)
        
        if n > 0:
            selected_idx = downsample_indices(np.arange(n_avail), n)
            all_selected_jets.extend(bin_array[selected_idx])
    
    if len(all_selected_jets) == 0:
        with h5py.File(out_path, "w") as fout:
            fout.create_dataset("Jets", shape=(0,), dtype=dtype)
        print(f"Saved {out_path} with 0 jets.")
        return
    
    combined_jets = np.array(all_selected_jets)
    
    print(f"Shuffling {len(combined_jets)} jets...")
    np.random.shuffle(combined_jets)
    
    with h5py.File(out_path, "w") as fout:
        out_dataset = fout.create_dataset("Jets", data=combined_jets)
    
    print(f"Saved {out_path} with {len(combined_jets)} jets (shuffled).")

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
    
+    if args.merge_type in ["realistic", "both"]:
        merge_realistic(entries, cols, eff_lumis, output_realistic)
    
    if args.merge_type in ["flat", "both"]:
        merge_flat(entries, cols, pt_edges, report_result["total_per_bin"], 
                   report_result["min_total_bin"], output_flat)

if __name__ == "__main__":
    main()
