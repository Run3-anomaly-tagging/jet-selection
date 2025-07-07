import h5py
import numpy as np
import math
from ImageUtils import make_image
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import time

def create_jet_images(h5_filename, npix=32, img_width=1.2, rotate=False):
    with h5py.File(h5_filename, "r+") as hf:
        lead_pfcands = hf["LeadPFCands/vectors"]
        sublead_pfcands = hf["SubleadPFCands/vectors"]

        lead_jet_kin = np.stack([
            hf["LeadFatJet/pt"][:],
            hf["LeadFatJet/eta"][:],
            hf["LeadFatJet/phi"][:],
            hf["LeadFatJet/mass"][:]
        ], axis=1)  # shape (n_events, 4)

        sublead_jet_kin = np.stack([
            hf["SubleadFatJet/pt"][:],
            hf["SubleadFatJet/eta"][:],
            hf["SubleadFatJet/phi"][:],
            hf["SubleadFatJet/mass"][:]
        ], axis=1)

        n_events = lead_pfcands.shape[0]
        batch_size = 5000
        n_batches = math.ceil(n_events / batch_size)

        # Prepare datasets for images, create or overwrite
        if "images" in hf["LeadFatJet"]:
            del hf["LeadFatJet/images"]
        if "images" in hf["SubleadFatJet"]:
            del hf["SubleadFatJet/images"]

        lead_images_ds = hf["LeadFatJet"].create_dataset(
            "images", shape=(0, npix, npix), maxshape=(None, npix, npix),
            dtype=np.float16, chunks=True, compression="gzip"
        )
        sublead_images_ds = hf["SubleadFatJet"].create_dataset(
            "images", shape=(0, npix, npix), maxshape=(None, npix, npix),
            dtype=np.float16, chunks=True, compression="gzip"
        )

        start_time = time.time()
        for i in range(n_batches):
            if(i>0):
                elapsed = time.time() - start_time
                print(f"Processed {i}/{n_batches} batches, elapsed {elapsed:.1f}s, avg {elapsed/(i*batch_size):.3f}s/image")
            start = i * batch_size
            end = min(n_events, (i + 1) * batch_size)

            lead_batch_pfcands = lead_pfcands[start:end]
            sublead_batch_pfcands = sublead_pfcands[start:end]
            lead_batch_kin = lead_jet_kin[start:end]
            sublead_batch_kin = sublead_jet_kin[start:end]

            lead_images = np.zeros((end - start, npix, npix), dtype=np.float16)
            sublead_images = np.zeros((end - start, npix, npix), dtype=np.float16)

            for j in range(end - start):
                lead_images[j] = make_image(
                    lead_batch_kin[j], lead_batch_pfcands[j],
                    npix=npix, img_width=img_width, norm=True, rotate=rotate
                )
            for j in range(end - start):
                sublead_images[j] = make_image(
                    sublead_batch_kin[j], sublead_batch_pfcands[j],
                    npix=npix, img_width=img_width, norm=True, rotate=rotate
                )

            # Append batches to datasets
            lead_images_ds.resize(lead_images_ds.shape[0] + lead_images.shape[0], axis=0)
            lead_images_ds[-lead_images.shape[0]:] = lead_images

            sublead_images_ds.resize(sublead_images_ds.shape[0] + sublead_images.shape[0], axis=0)
            sublead_images_ds[-sublead_images.shape[0]:] = sublead_images

    print(f"Jet images created and stored in {h5_filename} under LeadFatJet/images and SubleadFatJet/images.")

def plot_jet_images(h5_file_path, group='LeadFatJet', n_images=9):
    with h5py.File(h5_file_path, 'r') as f:
        images = f[group]['images'][:n_images]

    n_cols = 3
    n_rows = int(np.ceil(n_images / n_cols))
    fig, axs = plt.subplots(n_rows, n_cols, figsize=(10, 10), constrained_layout=True)

    vmin = np.min(0.001)
    vmax = np.max(1.)
    norm = colors.LogNorm(vmin=vmin, vmax=vmax)

    for idx, ax in enumerate(axs.flat):
        if idx >= len(images):
            ax.axis('off')
            continue
        img = images[idx]
        cmap = plt.get_cmap('viridis').copy()
        cmap.set_under(color='white')
        im = ax.imshow(img, norm=norm, cmap=cmap, origin='lower')
        ax.set_xticks([])
        ax.set_yticks([])
        ax.grid(True, which='both', color='gray', linestyle=':', linewidth=0.5)

    cbar = fig.colorbar(im, ax=axs, orientation='vertical', fraction=0.02, pad=0.04)
    cbar.set_label('Fraction of jet energy')

    plt.savefig("jet_images.png", dpi=150)
    print("Plotted jet images in jet_images.png")
