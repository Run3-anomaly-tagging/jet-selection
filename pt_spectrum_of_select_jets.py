#!/usr/bin/env python3

import os
import h5py
import numpy as np
import matplotlib.pyplot as plt
import mplhep as hep

hep.style.use("CMS")

# ─── INPUT FILES & HISTOGRAM BINS ────────────────────────────────────────
FileInfo = lambda label, path, xsec: {
    "label": label,
    "path": path,
    "xsec": xsec
}

files = [
    FileInfo("HT 400–600",    "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-400to600.h5", 95620),
    FileInfo("HT 600–800",    "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-600to800.h5", 13540),
    FileInfo("HT 800–1000",   "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-800to1000.h5", 3033),
    FileInfo("HT 1000–1200",  "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-1000to1200.h5", 883),
    FileInfo("HT 1200–1500",  "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-1200to1500.h5", 383),
    FileInfo("HT 1500–2000",  "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-1500to2000.h5", 125),
    FileInfo("HT 2000+",      "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/QCD_HT-2000toInf.h5", 26.5),
    #FileInfo("gg→H→bb̄",      "/uscms/home/ecakir/nobackup/jet_pt_h5/2022/GluGluHto2B.h5"),
]

# 100 bins from 0 to 2000 GeV
bin_edges = np.linspace(0, 2000, 101)

# ─── LOAD & HISTOGRAM ───────────────────────────────────────────────────
hist_data = []
for info in files:
    if not os.path.exists(info['path']):
        raise FileNotFoundError(f"File not found: {info['path']}")
    with h5py.File(info['path'], 'r') as f:
        jets = f['Jets'] 
        pt = jets['pt'][:]
        counts, _ = np.histogram(pt, bins=bin_edges)
        n_jets = counts.sum()
        #We are scaling xsec by number of jets, it is not perfect but looks good enough for now
        #Once we store the number of processed events, this will have to be changed
        scale = info['xsec'] / n_jets if n_jets > 0 else 0.0
        scaled_counts = counts * scale
        hist_data.append((scaled_counts, info['label']))
        print(f"{info['label']}: {n_jets:,} jets -> scale = {scale:.3e}")

# ─── PLOTTING ─────────────────────────────────────────────────────────────
fig, ax = plt.subplots()

counts_list = [h[0] for h in hist_data]
labels_list = [h[1] for h in hist_data]
hep.histplot([h[0] for h in hist_data], bins=bin_edges, histtype="fill", label=[h[1] for h in hist_data], stack=True, ax=ax)

# Axis labels
ticks = np.linspace(0, 2000, 5)
ax.set_xlabel(r"$p_{T}^{\mathrm{jet}}\;[\mathrm{GeV}]$")
ax.set_ylabel("Jets / bin")
ax.set_yscale("log")
ax.set_ylim(0.1, 5*10**5)
ax.set_xlim(150, 1500)
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