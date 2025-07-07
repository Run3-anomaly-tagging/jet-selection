import uproot
import numpy as np
import h5py
import time
import make_jet_images
import sys

MAX_EVENTS = -1 #For debugging
N_HIDDEN_LAYERS = 256
CREATE_IMAGES = True

#Sorts PFCands by pt, and builds pt,eta,phi,m vectors, pads/truncates to max_cands=100
def build_pfcand_vectors_ptetaphi(pt_arr, eta_arr, phi_arr, mass_arr, max_cands=100):
    print_interval = 1000
    start_time = time.time()

    n_events = MAX_EVENTS if MAX_EVENTS > 0 else len(pt_arr)
    output = np.zeros((n_events, max_cands, 4), dtype=np.float32)

    for ev in range(n_events):
        pts = pt_arr[ev]
        sorted_idx = np.argsort(pts)[::-1]
        sel = sorted_idx[:max_cands]

        n = len(sel)
        output[ev, :n, 0] = pt_arr[ev][sel]
        output[ev, :n, 1] = eta_arr[ev][sel]
        output[ev, :n, 2] = phi_arr[ev][sel]
        output[ev, :n, 3] = mass_arr[ev][sel]

        if ev > 0 and ev % print_interval == 0:
            elapsed = time.time() - start_time
            print(f"Processed {ev}/{n_events} events, elapsed {elapsed:.1f}s, avg {elapsed/ev:.3f}s/event")

    return output


def bunch_neuron_branches(tree, prefix, n_neurons=256):
    neuron_arrays = []
    for i in range(n_neurons):
        branch_name = f"{prefix}_globalParT3_hidNeuron{i:03d}"
        neuron_arrays.append(tree[branch_name].array().to_numpy().flatten())#They are of [n_events,1] shape
    neuron_matrix = np.vstack(neuron_arrays).T.astype(np.float32)
    return neuron_matrix

def main(input_file,output_file):
    with uproot.open(input_file) as f:
        tree = f["Events"] 


        LeadPFCands_pt = tree["LeadPFCands_pt"].array()
        LeadPFCands_eta = tree["LeadPFCands_eta"].array()
        LeadPFCands_phi = tree["LeadPFCands_phi"].array()
        LeadPFCands_mass = tree["LeadPFCands_mass"].array()

        SubleadPFCands_pt = tree["SubleadPFCands_pt"].array()
        SubleadPFCands_eta = tree["SubleadPFCands_eta"].array()
        SubleadPFCands_phi = tree["SubleadPFCands_phi"].array()
        SubleadPFCands_mass = tree["SubleadPFCands_mass"].array()

        LeadPFCands_vectors = build_pfcand_vectors_ptetaphi(
            LeadPFCands_pt, LeadPFCands_eta, LeadPFCands_phi, LeadPFCands_mass)

        SubleadPFCands_vectors = build_pfcand_vectors_ptetaphi(
            SubleadPFCands_pt, SubleadPFCands_eta, SubleadPFCands_phi, SubleadPFCands_mass)

        LeadFatJet_pt = tree["LeadFatJet_pt"].array()
        LeadFatJet_eta = tree["LeadFatJet_eta"].array()
        LeadFatJet_phi = tree["LeadFatJet_phi"].array()
        LeadFatJet_mass = tree["LeadFatJet_mass"].array()

        SubleadFatJet_pt = tree["SubleadFatJet_pt"].array()
        SubleadFatJet_eta = tree["SubleadFatJet_eta"].array()
        SubleadFatJet_phi = tree["SubleadFatJet_phi"].array()
        SubleadFatJet_mass = tree["SubleadFatJet_mass"].array()

        # Bunch hidNeuron branches for Lead and Sublead FatJet into one array [n_events,256] instead of 256 [n_events,1] arrays
        LeadFatJet_neurons = bunch_neuron_branches(tree, "LeadFatJet", N_HIDDEN_LAYERS)
        SubleadFatJet_neurons = bunch_neuron_branches(tree, "SubleadFatJet", N_HIDDEN_LAYERS)

        # Save all to HDF5
        with h5py.File(output_file, "w") as hf:
            lead_fatjet_grp = hf.create_group("LeadFatJet")
            lead_fatjet_grp.create_dataset("pt", data=LeadFatJet_pt)
            lead_fatjet_grp.create_dataset("eta", data=LeadFatJet_eta)
            lead_fatjet_grp.create_dataset("phi", data=LeadFatJet_phi)
            lead_fatjet_grp.create_dataset("mass", data=LeadFatJet_mass)
            lead_fatjet_grp.create_dataset("hidNeurons", data=LeadFatJet_neurons)

            sublead_fatjet_grp = hf.create_group("SubleadFatJet")
            sublead_fatjet_grp.create_dataset("pt", data=SubleadFatJet_pt)
            sublead_fatjet_grp.create_dataset("eta", data=SubleadFatJet_eta)
            sublead_fatjet_grp.create_dataset("phi", data=SubleadFatJet_phi)
            sublead_fatjet_grp.create_dataset("mass", data=SubleadFatJet_mass)
            sublead_fatjet_grp.create_dataset("hidNeurons", data=SubleadFatJet_neurons)

            lead_pfcand_grp = hf.create_group("LeadPFCands")
            lead_pfcand_grp.create_dataset("vectors", data=LeadPFCands_vectors)  # shape (n_events, 100, 4)

            sublead_pfcand_grp = hf.create_group("SubleadPFCands")
            sublead_pfcand_grp.create_dataset("vectors", data=SubleadPFCands_vectors)

    print(f"Data saved to {output_file}")

    if CREATE_IMAGES:
        print("Creating jet images")
        make_jet_images.create_jet_images(output_file)
        make_jet_images.plot_jet_images(output_file, group='LeadFatJet', n_images=9)
    with h5py.File(output_file, "r") as hf:
        print_h5_structure(hf)

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
        print("Usage: python script.py input_file.root output_file.root")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file,output_file)
