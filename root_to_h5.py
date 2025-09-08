import uproot
import numpy as np
import h5py
import time
import make_jet_images
import sys
from collections import defaultdict

MAX_EVENTS = -1  # limit events for debugging (-1 to disable)
N_HIDDEN_LAYERS = 256
CREATE_IMAGES = True
MAX_PFCANDS = 100
NPIX = 32

def build_grouped_pfcands(pt, eta, phi, mass, jetIdx, max_cands=MAX_PFCANDS):
    n_events = len(pt)
    grouped = []

    for ev in range(n_events):
        jets_pfcands = defaultdict(list)
        for i, jet_i in enumerate(jetIdx[ev]):
            jets_pfcands[jet_i].append((pt[ev][i], eta[ev][i], phi[ev][i], mass[ev][i]))

        event_output = []
        for jet in sorted(jets_pfcands.keys()):
            cands = sorted(jets_pfcands[jet], key=lambda x: x[0], reverse=True)[:max_cands]
            cand_array = np.zeros((max_cands, 4), dtype=np.float32)
            for i, (pt_, eta_, phi_, m_) in enumerate(cands):
                cand_array[i] = [pt_, eta_, phi_, m_]
            event_output.append(cand_array)

        grouped.append(event_output)
    return grouped  # [n_events][n_jets_per_event][max_cands, 4]

def bunch_neuron_branches(tree, prefix, n_hidden, entry_stop=None):
    branch_names = [f"{prefix}_globalParT3_hidNeuron{i:03d}" for i in range(n_hidden)]
    arrays = tree.arrays(branch_names, entry_stop=entry_stop, library="np")
    return arrays, branch_names


def main(input_file, output_file):
    start_time = time.time()
    with uproot.open(input_file) as f:
        tree = f["Events"]

        # Limit events if debugging
        n_events_total = len(tree["SelectedFatJet_pt"].array())
        n_events = n_events_total if MAX_EVENTS < 0 else min(MAX_EVENTS, n_events_total)

        # Load jet-level info
        FatJet_pt = tree["SelectedFatJet_pt"].array(entry_stop=n_events)
        FatJet_eta = tree["SelectedFatJet_eta"].array(entry_stop=n_events)
        FatJet_phi = tree["SelectedFatJet_phi"].array(entry_stop=n_events)
        FatJet_mass = tree["SelectedFatJet_mass"].array(entry_stop=n_events)

        PFCands_pt = tree["SelectedPFCands_pt"].array(entry_stop=n_events)
        PFCands_eta = tree["SelectedPFCands_eta"].array(entry_stop=n_events)
        PFCands_phi = tree["SelectedPFCands_phi"].array(entry_stop=n_events)
        PFCands_mass = tree["SelectedPFCands_mass"].array(entry_stop=n_events)
        PFCands_jetIdx = tree["SelectedPFCands_jetMatchIdx"].array(entry_stop=n_events)

        # Build grouped PFCands
        GroupedPFCands = build_grouped_pfcands(PFCands_pt, PFCands_eta, PFCands_phi,
                                               PFCands_mass, PFCands_jetIdx, MAX_PFCANDS)

        # Get neurons for jets [n_events, n_jets, n_neurons]
        FatJet_neurons, neuron_branch_names = bunch_neuron_branches(tree, "SelectedFatJet", N_HIDDEN_LAYERS, entry_stop=n_events)        
        
        # Flatten jets across events
        jets_list = []
        jet_counter = 0
        total_jets = sum(len(jets) for jets in GroupedPFCands)

        print(f"Total jets in {n_events} events: {total_jets}")

        for ev in range(n_events):
            n_jets = len(GroupedPFCands[ev])
            for jet_i in range(n_jets):
                if jet_counter >= total_jets:
                    break

                fatjet_info = (
                    FatJet_pt[ev][jet_i],
                    FatJet_eta[ev][jet_i],
                    FatJet_phi[ev][jet_i],
                    FatJet_mass[ev][jet_i],
                )
                pfcands = GroupedPFCands[ev][jet_i]  # shape (max_cands,4)

                neurons = np.array([FatJet_neurons[name][ev][jet_i] for name in neuron_branch_names])

                # Placeholder for jet image, fill later or set zeros
                jet_image = np.zeros((NPIX,NPIX), dtype=np.float32)

                jets_list.append((fatjet_info, pfcands, neurons, jet_image))

                jet_counter += 1
            if jet_counter >= total_jets:
                break

        print(f"Processed {jet_counter}/{total_jets} jets")

        # Define compound dtype for HDF5 dataset
        dtype = np.dtype([
            ("pt", np.float32),
            ("eta", np.float32),
            ("phi", np.float32),
            ("mass", np.float32),
            ("pfcands", np.float32, (MAX_PFCANDS, 4)),
            ("hidNeurons", np.float32, (N_HIDDEN_LAYERS,)),
            ("jet_image", np.float32, (NPIX,NPIX)),
        ])

        jets_array = np.zeros(len(jets_list), dtype=dtype)

        for i, (fatjet_info, pfcands, neurons, jet_image) in enumerate(jets_list):
            jets_array[i]["pt"] = fatjet_info[0]
            jets_array[i]["eta"] = fatjet_info[1]
            jets_array[i]["phi"] = fatjet_info[2]
            jets_array[i]["mass"] = fatjet_info[3]
            jets_array[i]["pfcands"] = pfcands
            jets_array[i]["hidNeurons"] = neurons
            jets_array[i]["jet_image"] = jet_image

        with h5py.File(output_file, "w") as hf:
            hf.create_dataset("Jets", data=jets_array)

    elapsed = time.time() - start_time
    print(f"Saved {len(jets_list)} jets to {output_file} in {elapsed:.1f}s ({elapsed/len(jets_list):.3f} jet/s)")

    if CREATE_IMAGES:
        print("Creating jet images")
        make_jet_images.create_jet_images(output_file)
        make_jet_images.plot_jet_images(output_file, group='Jets', n_images=9)

    with h5py.File(output_file, "r") as hf:
        print_h5_structure(hf)
        sanityCheck = False
        if sanityCheck:
            print("\n--- First Jet Contents ---")
            jet0 = hf["Jets"][0]
            np.set_printoptions(threshold=np.inf, linewidth=200,formatter={'float_kind': lambda x: f"{x:.2f}"})
            for field in jet0.dtype.names:
                print(f"{field}:\n{jet0[field]}\n")


def print_h5_structure(h5file, group_name="/", indent=0):
    group = h5file[group_name]
    for key, item in group.items():
        print("  " * indent + f"{key}: ", end="")
        if isinstance(item, h5py.Dataset):
            print(f"Dataset, shape: {item.shape}, dtype: {item.dtype}")
        elif isinstance(item, h5py.Group):
            print("Group")
            print_h5_structure(h5file, group_name + key + "/", indent + 1)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python root_to_h5.py input_file.root output_file.h5")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)
