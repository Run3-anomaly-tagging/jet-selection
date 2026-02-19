import uproot
import numpy as np
import h5py
import time
import sys

MAX_EVENTS = -1  # limit events for debugging (-1 to disable)
N_HIDDEN_LAYERS = 256

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
        FatJet_mass = tree["SelectedFatJet_msoftdrop"].array(entry_stop=n_events)
        FatJet_globalParT3_QCD = tree["SelectedFatJet_globalParT3_QCD"].array(entry_stop=n_events)
        FatJet_globalParT3_TopbWqq = tree["SelectedFatJet_globalParT3_TopbWqq"].array(entry_stop=n_events)
        FatJet_globalParT3_TopbWq = tree["SelectedFatJet_globalParT3_TopbWq"].array(entry_stop=n_events)
        FatJet_particleNet_QCD0HF = tree["SelectedFatJet_particleNet_QCD0HF"].array(entry_stop=n_events)
        FatJet_particleNet_QCD1HF = tree["SelectedFatJet_particleNet_QCD1HF"].array(entry_stop=n_events)
        FatJet_particleNet_QCD2HF = tree["SelectedFatJet_particleNet_QCD2HF"].array(entry_stop=n_events)
        
        # Optional branches
        FatJet_top_cat = None
        FatJet_hadronFlavour = None
        if "SelectedFatJet_top_cat" in tree.keys():
            FatJet_top_cat = tree["SelectedFatJet_top_cat"].array(entry_stop=n_events)
        if "SelectedFatJet_hadronFlavour" in tree.keys():
            FatJet_hadronFlavour = tree["SelectedFatJet_hadronFlavour"].array(entry_stop=n_events)



        # Get neurons for jets [n_events, n_jets, n_neurons]
        FatJet_neurons, neuron_branch_names = bunch_neuron_branches(tree, "SelectedFatJet", N_HIDDEN_LAYERS, entry_stop=n_events)        
        
        # Flatten jets across events
        jets_list = []
        jet_counter = 0
        total_jets = sum(len(jets) for jets in FatJet_pt)

        print(f"Total jets in {n_events} events: {total_jets}")

        for ev in range(n_events):
            n_jets = len(FatJet_pt[ev])
            for jet_i in range(n_jets):
                if jet_counter >= total_jets:
                    break

                top_category = FatJet_top_cat[ev][jet_i] if FatJet_top_cat is not None else -1
                hadron_flavour = FatJet_hadronFlavour[ev][jet_i] if FatJet_hadronFlavour is not None else -1
                particleNet_QCD0HF = FatJet_particleNet_QCD0HF[ev][jet_i]
                particleNet_QCD1HF = FatJet_particleNet_QCD1HF[ev][jet_i]
                particleNet_QCD2HF = FatJet_particleNet_QCD2HF[ev][jet_i]
                fatjet_info = (
                    FatJet_pt[ev][jet_i],
                    FatJet_eta[ev][jet_i],
                    FatJet_phi[ev][jet_i],
                    FatJet_mass[ev][jet_i],
                    top_category,
                    hadron_flavour,
                    particleNet_QCD0HF,
                    particleNet_QCD1HF,
                    particleNet_QCD2HF,
                    FatJet_globalParT3_QCD[ev][jet_i],
                    FatJet_globalParT3_TopbWqq[ev][jet_i],
                    FatJet_globalParT3_TopbWq[ev][jet_i],
                )

                neurons = np.array([FatJet_neurons[name][ev][jet_i] for name in neuron_branch_names])
                jets_list.append((fatjet_info, neurons))

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
            ("hidNeurons", np.float32, (N_HIDDEN_LAYERS,)),
            ("top_category", np.int32),
            ("hadron_flavour", np.int32),
            ("particleNet_QCD0HF", np.float32),
            ("particleNet_QCD1HF", np.float32),
            ("particleNet_QCD2HF", np.float32),
            ("globalParT3_QCD", np.float32),
            ("globalParT3_TopbWqq", np.float32),
            ("globalParT3_TopbWq", np.float32)
        ])

        jets_array = np.zeros(len(jets_list), dtype=dtype)

        for i, (fatjet_info, neurons) in enumerate(jets_list):
            jets_array[i]["pt"] = fatjet_info[0]
            jets_array[i]["eta"] = fatjet_info[1]
            jets_array[i]["phi"] = fatjet_info[2]
            jets_array[i]["mass"] = fatjet_info[3]
            jets_array[i]["top_category"] = fatjet_info[4]
            jets_array[i]["hadron_flavour"] = fatjet_info[5]
            jets_array[i]["particleNet_QCD0HF"] = fatjet_info[6]
            jets_array[i]["particleNet_QCD1HF"] = fatjet_info[7]
            jets_array[i]["particleNet_QCD2HF"] = fatjet_info[8]
            jets_array[i]["globalParT3_QCD"] = fatjet_info[9]
            jets_array[i]["globalParT3_TopbWqq"] = fatjet_info[10]
            jets_array[i]["globalParT3_TopbWq"] = fatjet_info[11]
            jets_array[i]["hidNeurons"] = neurons

        with h5py.File(output_file, "w") as hf:
            hf.create_dataset("Jets", data=jets_array)

    elapsed = time.time() - start_time
    print(f"Saved {len(jets_list)} jets to {output_file} in {elapsed:.1f}s ({elapsed/len(jets_list):.3f} jet/s)")

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
