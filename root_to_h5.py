import uproot
import numpy as np
import h5py
import ROOT
import time
import make_jet_images

MAX_EVENTS = -1 #For debugging
N_HIDDEN_LAYERS = 256
CREATE_IMAGES = True

# Builds PFCands pt,eta,phi,m vectors and converts to (px, py, pz, E), pads/truncates to max_cands=100
def build_pfcand_vectors_ptetaphi(pt_arr, eta_arr, phi_arr, mass_arr, max_cands=100):
    pfcand_vectors = []
    print_interval = 1000  # print status every 1000 events
    start_time = time.time()
    #for ev in range(n_events):
    if MAX_EVENTS>0:
        n_events = MAX_EVENTS
    else:
        n_events = len(pt_arr)
    for ev in range(n_events):
        if ev > 0 and ev % print_interval == 0:
            elapsed = time.time() - start_time
            print(f"Processed {ev}/{n_events} events, elapsed {elapsed:.1f}s, avg {elapsed/ev:.3f}s/event")
        pts = pt_arr[ev]

        # Get sorting indices descending by pt
        sorted_indices = np.argsort(pts)[::-1]

        pts_sorted = pts[sorted_indices]
        eta_sorted = eta_arr[ev][sorted_indices]
        phi_sorted = phi_arr[ev][sorted_indices]
        mass_sorted = mass_arr[ev][sorted_indices]

        event_vectors = []
        n_cands = len(pts_sorted)
        for i in range(min(n_cands, max_cands)):
            cand = ROOT.Math.PtEtaPhiMVector(
                pts_sorted[i], eta_sorted[i], phi_sorted[i], mass_sorted[i]
            )
            event_vectors.append([cand.Px(), cand.Py(), cand.Pz(), cand.E()])

        # Zero padding
        for _ in range(max_cands - len(event_vectors)):
            event_vectors.append([0., 0., 0., 0.])

        pfcand_vectors.append(event_vectors)

    return np.array(pfcand_vectors, dtype=np.float32)

def bunch_neuron_branches(tree, prefix, n_neurons=256):
    neuron_arrays = []
    for i in range(n_neurons):
        branch_name = f"{prefix}_globalParT3_hidNeuron{i:03d}"
        neuron_arrays.append(tree[branch_name].array().to_numpy().flatten())#They are of [n_events,1] shape
    neuron_matrix = np.vstack(neuron_arrays).T.astype(np.float32)
    return neuron_matrix

def main():
    input_file = "test.root"
    output_file = "output.h5"


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
        make_jet_images.create_jet_images(output_file)
    
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
    main()
