import h5py
import numpy as np
import math
from ImageUtils import make_image
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import time

def create_jet_images(h5_filename, npix=32, img_width=1.2, rotate=False):
    with h5py.File(h5_filename, "r+") as hf:
        jets_ds = hf["Jets"]
        n_jets = jets_ds.shape[0]
        batch_size = 5000
        n_batches = math.ceil(n_jets / batch_size)

        start_time = time.time()
        for i in range(n_batches):
            if i > 0:
                elapsed = time.time() - start_time
                print(f"Processed {i * batch_size}/{n_jets} jets, elapsed {elapsed:.1f}s, avg {elapsed / (i * batch_size):.3f}s/image")

            start = i * batch_size
            end = min(n_jets, (i + 1) * batch_size)

            jets_batch = jets_ds[start:end]

            pt = jets_batch["pt"]
            eta = jets_batch["eta"]
            phi = jets_batch["phi"]
            mass = jets_batch["mass"]
            pfcands = jets_batch["pfcands"]

            images = np.zeros((end - start, npix, npix), dtype=np.float32)

            for j in range(end - start):
                jet_kin = np.array([pt[j], eta[j], phi[j], mass[j]], dtype=np.float32)
                image = make_image(
                    jet_kin, pfcands[j],
                    npix=npix, img_width=img_width,
                    norm=True, rotate=rotate
                )
                if np.all(image == 0):
                    print(f"Empty image for jet index {start + j}")
                images[j] = image

            jets_batch["jet_image"] = images
            jets_ds[start:end] = jets_batch

        print(f"Jet images created and stored in-place under 'Jets/jet_image'")


def plot_jet_images(h5_file_path, group='LeadFatJet', n_images=9):
    with h5py.File(h5_file_path, 'r') as f:
        images = f[group]['jet_image'][:n_images]
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
