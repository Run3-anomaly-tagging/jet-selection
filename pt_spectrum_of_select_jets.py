#!/usr/bin/env python3

import os
import h5py
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep

# ─── GLOBAL STYLE SETTINGS ────────────────────────────────────────────────
plt.rcParams.update({
    "figure.figsize":   (8, 6),    # width, height in inches
    "figure.dpi":       150,       # dots per inch
    "font.family":      "sans-serif",
    "font.size":        14,        # base font size
    "axes.titlesize":   16,
    "axes.labelsize":   16,
    "xtick.labelsize":  12,
    "ytick.labelsize":  12,
    "legend.fontsize":  14,
})

# Apply CMS style
hep.style.use("CMS")

# ─── INPUT FILES & HISTOGRAM BINS ────────────────────────────────────────
# Update these paths to match your location
FileInfo = lambda label, path: {
    "label": label,
    "path": os.path.expanduser(path),
}

files = [
    #FileInfo("HT 600–800",    "~/nobackup/jet_pt_h5/2022/QCD_HT-600to800.h5"),
    #FileInfo("HT 800–1000",   "~/nobackup/jet_pt_h5/2022/QCD_HT-800to1000.h5"),
    #FileInfo("HT 1000–1200",  "~/nobackup/jet_pt_h5/2022/QCD_HT-1000to1200.h5"),
    #FileInfo("HT 1200–1500",  "~/nobackup/jet_pt_h5/2022/QCD_HT-1200to1500.h5"),
    #FileInfo("HT 1500–2000",  "~/nobackup/jet_pt_h5/2022/QCD_HT-1500to2000.h5"),
    #FileInfo("HT 2000+",      "~/nobackup/jet_pt_h5/2022/QCD_HT-2000toInf.h5"),
    FileInfo("gg→H→bb̄",      "~/nobackup/jet_pt_h5/2022/GluGluHto2B.h5"),
]

# 100 bins from 0 to 2000 GeV
bin_edges = np.linspace(0, 2000, 101)

# ─── LOAD & HISTOGRAM ───────────────────────────────────────────────────
hist_data = []
for info in files:
    if not os.path.exists(info['path']):
        raise FileNotFoundError(f"File not found: {info['path']}")
    with h5py.File(info['path'], 'r') as f:
        jets = f['Jets']           # structured dataset
        pt = jets['pt'][:]         # array of jet pT values
    counts, _ = np.histogram(pt, bins=bin_edges)
    hist_data.append((counts, info['label']))
    print(f"{info['label']}: {counts.sum():,} jets binned")

# ─── PLOTTING ─────────────────────────────────────────────────────────────
fig, ax = plt.subplots()
for counts, label in hist_data:
    hep.histplot(counts,
                 bins=bin_edges,
                 histtype="fill",
                 label=label)

# Axis labels
ticks = np.linspace(0, 2000, 5)
ax.set_xlabel(r"$p_{T}^{\mathrm{jet}}\;[\mathrm{GeV}]$")
ax.set_ylabel("Jets / bin")
ax.set_yscale("log")

# Legend
leg = ax.legend(loc="best")
leg.get_frame().set_alpha(0.9)

# CMS label: Simulation WiP, energy only
hep.cms.label("WiP", data=False, com=13.6, ax=ax)

plt.tight_layout()
outfile = "jet_pt_normalized_pretty.png"
plt.savefig(outfile)
print(f"Saved plot → {outfile}")
plt.show()