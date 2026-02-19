from TIMBER.Analyzer import *
from TIMBER.Tools.Common import *
import ROOT, sys

ROOT.gROOT.SetBatch(True)

# Adaptation of selection code to force flat mass spectrum for QCD jets.
# Produces a mass histogram of selected jets, calculates the bin count in 245--250 GeV mass bin and set this as maximum number of jets in any 5 GeV mass bin

sys.path.append('../../')

default_cuts = {
    'pt_min': 170.,
    'abs_eta_max': 2.4,
    'mass_min': 40.,
    'deltaR_max': 0.8  # For DeltaR matching
}

def get_max_count(input_file, cuts):
    # Get histogram of jet mass after cuts and return max count in 5 GeV bins between 245 and 250 GeV
    f = ROOT.TFile(input_file)
    tree = f.Get('Events')
    hist = ROOT.TH1F('temp_hist', '', 100, 0, 500)
    pt_cut = cuts['pt_min']
    eta_cut = cuts['abs_eta_max']
    mass_cut = cuts['mass_min']
    tree.Draw('FatJet_msoftdrop>>temp_hist', f"FatJet_pt>{pt_cut} && abs(FatJet_eta)<{eta_cut} && FatJet_msoftdrop>{mass_cut}")
    count = 0
    for i in range(1, hist.GetNbinsX() + 1):
        bin_center = hist.GetBinCenter(i)
        if 245 <= bin_center < 250:
            count = hist.GetBinContent(i)
    f.Close()
    return count

if len(sys.argv) < 3:
    print("Usage: python selection.py input_file.root output_file.root")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

CompileCpp('TIMBER/Framework/include/common.h')
CompileCpp('TIMBER_modules/FlatMass.cc')

a = analyzer(input_file)

etaCut = default_cuts["abs_eta_max"]
ptCut = default_cuts["pt_min"]
massCut = default_cuts["mass_min"]

max_count = get_max_count(input_file, default_cuts)
print(f"Jet count in 245-250 GeV mSD bin: {max_count}")

# Create a single instance of MassFlattener
# Using a.Define(massFlattener, "MassFlattener()") would create a new instance for each event, which we do not want!
ROOT.gInterpreter.ProcessLine("MassFlattener mass_flattener;")

myCuts = CutGroup('myCuts')
a.Cut('njet', 'nFatJet>0')
a.Cut('pt_cut', f'FatJet_pt[0] > {ptCut}')  # Ordered in pT so we can apply cut on first jet

# Use the MassFlattener instance to define selected_jet_indices
a.Define("selected_jet_indices", f"mass_flattener.SelectJetsFlatMass(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut}, {max_count})")

# Remove events without jets to avoid crashing
a.Cut("has_selected_jets", "selected_jet_indices.size() > 0")

keep_list = ["pt", "phi", "eta", "msoftdrop", "globalParT3_hidNeuron", "globalParT3_QCD", "globalParT3_TopbWqq", "globalParT3_TopbWq", "hadronFlavour", "particleNet_QCD"]

a.SubCollection("SelectedFatJet", "FatJet", 'selected_jet_indices', useTake=True, keep=keep_list)

out_vars = ['nSelectedFatJet', 'SelectedFatJet*', 'SelectedFatJet_globalParT3*']
a.GetActiveNode().Snapshot(out_vars, output_file, 'Events', lazy=False, openOption='RECREATE')