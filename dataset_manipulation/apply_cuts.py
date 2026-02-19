import os
import argparse
import h5py
import numpy as np

def write_subset(out_path, jets):
    with h5py.File(out_path, "w") as fout:
        fout.create_dataset("Jets", data=jets)

def apply_qcd_cuts(input_path, output_dir):
    with h5py.File(input_path, "r") as fin:
        jets = fin["Jets"][:]

    if "hadron_flavour" not in jets.dtype.names:
        raise ValueError("Missing 'hadron_flavour' in input file.")

    masks = {
        "QCD_light": jets["hadron_flavour"] == 0,
        "QCD_c": jets["hadron_flavour"] == 4,
        "QCD_b": jets["hadron_flavour"] == 5,
    }

    for name, mask in masks.items():
        out_path = os.path.join(output_dir, f"{name}_flat.h5")
        write_subset(out_path, jets[mask])
        print(f"Saved {out_path} with {mask.sum()} jets.")

def apply_ttto4q_cuts(input_path, output_dir):
    with h5py.File(input_path, "r") as fin:
        jets = fin["Jets"][:]

    if "top_category" not in jets.dtype.names:
        raise ValueError("Missing 'top_category' in input file.")

    masks = {
        "Top_bqq": jets["top_category"] == 3,
        "Top_bq": jets["top_category"] == 2,
        "Top_qq": jets["top_category"] == 1,
    }

    for name, mask in masks.items():
        out_path = os.path.join(output_dir, f"{name}.h5")
        write_subset(out_path, jets[mask])
        print(f"Saved {out_path} with {mask.sum()} jets.")

def main():
    parser = argparse.ArgumentParser(description="Apply cuts to merged HDF5 files.")
    parser.add_argument("--input", required=True, help="Input .h5 file (merged output).")
    parser.add_argument("--output-dir", required=False, default="../data/", help="Directory for output .h5 files.")
    parser.add_argument("--dataset", choices=["QCD", "TTTo4Q"], required=True, help="Dataset type.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)

    if args.dataset == "QCD":
        apply_qcd_cuts(args.input, args.output_dir)
    elif args.dataset == "TTTo4Q":
        apply_ttto4q_cuts(args.input, args.output_dir)

if __name__ == "__main__":
    main()
