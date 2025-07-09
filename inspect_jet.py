# inspect_jet.py

import h5py
import sys
import numpy as np

def inspect_jet(h5_filename, jet_index):
    with h5py.File(h5_filename, "r") as hf:
        jets = hf["Jets"]

        if jet_index >= len(jets):
            print(f"Index {jet_index} out of bounds. Dataset has {len(jets)} jets.")
            return

        jet = jets[jet_index]

        print(f"Jet #{jet_index}")
        print("-" * 40)
        print(f"pt:   {jet['pt']}")
        print(f"eta:  {jet['eta']}")
        print(f"phi:  {jet['phi']}")
        print(f"mass: {jet['mass']}")

        #print("\nNeurons:")
        #print(jet['hidNeurons'])

        print("\nPFCands (non-zero only):")
        nonzero_mask = np.any(jet['pfcands'] != 0, axis=1)
        nonzero_pfcands = jet['pfcands'][nonzero_mask]
        print(nonzero_pfcands)

        print("\nJet image:")
        image = jet['jet_image']
        print(f"Image shape: {image.shape}")
        print(f"Min: {np.min(image)}, Max: {np.max(image)}, Non-zero pixels: {np.count_nonzero(image)}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python inspect_jet.py <file.h5> <jet_index>")
    else:
        h5_file = sys.argv[1]
        index = int(sys.argv[2])
        inspect_jet(h5_file, index)
