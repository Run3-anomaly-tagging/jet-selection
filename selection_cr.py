from TIMBER.Analyzer import *
from TIMBER.Tools.Common import *
import ROOT,sys

sys.path.append('../../')

default_cuts = {
    'pt_min':200.,
    'abs_eta_max':2.4,
    'mass_min':40.,
    'deltaR_max':0.8, #For DeltaR matching
    'pt_photon_min':250.,
    'tag_lepton_pt_min':40.,
    'st_min_top_region':600.,
    'st_min_qcd_region':1100.,
    'veto_lepton_pt_min':20.,
    'mu_iso_cut': 2, #MiniIso Medium
}

#PNet (uParT is not availble in NanoAOD version <15)
#WPs https://btv-wiki.docs.cern.ch/ScaleFactors/Run3Summer22/#ak4-b-tagging
b_tag_wp_m = {
    "2022": 0.245,
    "2022EE": 0.2605
}

def get_n_events(a):
    """Return the number of events for the active TIMBER node."""
    return a.DataFrame.Count().GetValue()


def record_cutflow_step(cutflow_labels, cutflow_counts, label, analyzer_instance):
    """Store a cut label and post-cut event count."""
    cutflow_labels.append(str(label))
    cutflow_counts.append(float(get_n_events(analyzer_instance)))


def apply_cut_with_count(analyzer_instance, cut_name, cut_expression, cutflow_labels, cutflow_counts):
    """Apply a cut and immediately record the remaining number of events."""
    analyzer_instance.Cut(cut_name, cut_expression)
    record_cutflow_step(cutflow_labels, cutflow_counts, cut_name, analyzer_instance)

if len(sys.argv) < 5:
    print("Usage: python selection.py input_file.root output_file.root region year")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]
region = sys.argv[3]
if region not in ["electron", "qcd", "muon"]:
    print("Invalid region specified. Must be 'electron', 'qcd', or 'muon'.")
    sys.exit(1)
year = sys.argv[4]

# Import the C++
CompileCpp('TIMBER/Framework/include/common.h')
CompileCpp('TIMBER_modules/JetMatching.cc')
CompileCpp('TIMBER_modules/TopControlRegion.cc')

# Create analyzer instance
a = analyzer(input_file)

cutflow_labels = []
cutflow_counts = []
record_cutflow_step(cutflow_labels, cutflow_counts, "Initial", a)

etaCut = default_cuts["abs_eta_max"]
ptCut = default_cuts["pt_min"]
massCut = default_cuts["mass_min"]
photonPtCut = default_cuts["pt_photon_min"]

if region == "qcd":
    #QCD selection
    '''
    HLT_Photon200
    nPhoton > 0
    Photon_pt[0] > 250
    ST > 800 GeV
    Delta R(Photon, jet) > 0.8
    '''


    apply_cut_with_count(a, "Trigger_cut", "HLT_Photon200==1", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "nPhoton_cut", "nPhoton>0", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, 'njet_cut', "nFatJet>0", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "photon_pt_cut", f"Photon_pt[0] > {photonPtCut}", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, 'pt_cut', f'FatJet_pt[0] > {ptCut}', cutflow_labels, cutflow_counts)#Ordered in pT so we can apply cut on first jet
    a.Define("ST", "CalculateST(Jet_pt, Photon_pt, PFMET_pt)") # AK4 jets used in calculation
    apply_cut_with_count(a, "ST_cut", f'ST > {default_cuts["st_min_qcd_region"]}', cutflow_labels, cutflow_counts)

    a.Define("leading_photon_eta", "Photon_eta[0]")
    a.Define("leading_photon_phi", "Photon_phi[0]")
    a.Define("selected_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut}, leading_photon_eta, leading_photon_phi, {default_cuts['deltaR_max']})")
elif region == "electron":
    #top (electron) selection
    '''
    #HLT_Ele30_WPTight_Gsf
    #nElectron > 0 
    #Tag electron pt > 40
    #ST>1100
    #nJet > 3
    #nbJet == 2
    #nFatJet > 0
    #lepton_veto
    dr(FatJet_probe, probe lepton) > 0.8
    '''
    apply_cut_with_count(a, "Trigger_cut", "HLT_Ele30_WPTight_Gsf==1", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "nElectron_cut", "nElectron>0", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "nJet_cut", "nJet>3 && nFatJet>0", cutflow_labels, cutflow_counts)
    a.Define("ST", "CalculateST(Jet_pt, ROOT::VecOps::RVec<float>{}, PFMET_pt)") # ST without photon
    apply_cut_with_count(a, "ST_cut", f'ST > {default_cuts["st_min_top_region"]}', cutflow_labels, cutflow_counts)
    a.Define("nbJet", f"nBJets(Jet_btagPNetB, {b_tag_wp_m[year]})")
    apply_cut_with_count(a, "nbJet_cut", "nbJet==2", cutflow_labels, cutflow_counts)
    a.Define("tag_electron_idx", f"tagElectronIndex(Electron_pt, Electron_eta, Electron_mvaIso_WP90, {default_cuts['tag_lepton_pt_min']}, {default_cuts['abs_eta_max']})")
    apply_cut_with_count(a, "tag_electron_cut", "tag_electron_idx >= 0", cutflow_labels, cutflow_counts)
    a.Define("tag_lepton_eta", "Electron_eta[tag_electron_idx]")
    a.Define("tag_lepton_phi", "Electron_phi[tag_electron_idx]")
    a.Define("n_veto_electrons", f"nVetoElectrons(Electron_pt, Electron_eta, Electron_mvaIso_WP90, {default_cuts['veto_lepton_pt_min']}, {default_cuts['abs_eta_max']})")
    a.Define("n_veto_muons", f"nVetoMuons(Muon_pt, Muon_eta, Muon_miniIsoId, {default_cuts['veto_lepton_pt_min']}, {default_cuts['abs_eta_max']}, {default_cuts['mu_iso_cut']})")
    apply_cut_with_count(a, "lepton_veto_cut", "n_veto_electrons==1 && n_veto_muons==0", cutflow_labels, cutflow_counts) #Probe lepton should be only lepton in event
    a.Define("selected_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut}, tag_lepton_eta, tag_lepton_phi, {default_cuts['deltaR_max']})")
elif region == "muon":
    #top (electron) selection
    '''
    #HLT_IsoMu24_v
    #nMuon > 0 
    #Tag Muon_pt > 40
    #ST>1100
    #nJet > 3
    #nbJet == 2
    #nFatJet > 0
    #lepton_veto
    dr(FatJet_probe, probe lepton) > 0.8
    '''
    apply_cut_with_count(a, "Trigger_cut", "HLT_IsoMu24==1", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "nMuon_cut", "nMuon>0", cutflow_labels, cutflow_counts)
    apply_cut_with_count(a, "nJet_cut", "nJet>3 && nFatJet>0", cutflow_labels, cutflow_counts)
    a.Define("ST", "CalculateST(Jet_pt, ROOT::VecOps::RVec<float>{}, PFMET_pt)") # ST without photon
    apply_cut_with_count(a, "ST_cut", f'ST > {default_cuts["st_min_top_region"]}', cutflow_labels, cutflow_counts)
    a.Define("nbJet", f"nBJets(Jet_btagPNetB, {b_tag_wp_m[year]})")
    apply_cut_with_count(a, "nbJet_cut", "nbJet==2", cutflow_labels, cutflow_counts)
    a.Define("tag_muon_idx", f"tagMuonIndex(Muon_pt, Muon_eta, Muon_miniIsoId, {default_cuts['tag_lepton_pt_min']}, {default_cuts['abs_eta_max']}, {default_cuts['mu_iso_cut']})")
    apply_cut_with_count(a, "tag_muon_cut", "tag_muon_idx >= 0", cutflow_labels, cutflow_counts)
    a.Define("tag_lepton_eta", "Muon_eta[tag_muon_idx]")
    a.Define("tag_lepton_phi", "Muon_phi[tag_muon_idx]")
    a.Define("n_veto_electrons", f"nVetoElectrons(Electron_pt, Electron_eta, Electron_mvaIso_WP90, {default_cuts['veto_lepton_pt_min']}, {default_cuts['abs_eta_max']})")
    a.Define("n_veto_muons", f"nVetoMuons(Muon_pt, Muon_eta, Muon_miniIsoId, {default_cuts['veto_lepton_pt_min']}, {default_cuts['abs_eta_max']}, {default_cuts['mu_iso_cut']})")
    apply_cut_with_count(a, "lepton_veto_cut", "n_veto_electrons==0 && n_veto_muons==1", cutflow_labels, cutflow_counts) #Probe lepton should be only lepton in event
    a.Define("selected_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut}, tag_lepton_eta, tag_lepton_phi, {default_cuts['deltaR_max']})")
else:
    print("How did we get here? This should have been caught by the earlier region check.")
    print("Region specified:", region)
    sys.exit(1)

#Remove events without jets
apply_cut_with_count(a, "has_selected_jets", "selected_jet_indices.size() > 0", cutflow_labels, cutflow_counts) 
#We keep up to two jets after selection
a.Define("pruned_selected_jet_indices", "TruncateIndices(selected_jet_indices,2)")

keep_list = ["pt", "phi", "eta", "msoftdrop","globalParT3_hidNeuron","globalParT3_QCD","globalParT3_TopbWqq","globalParT3_TopbWq","hadronFlavour","particleNet_QCD"]

a.SubCollection("SelectedFatJet", "FatJet",'pruned_selected_jet_indices',useTake=True, keep=keep_list)

out_vars = ['nSelectedFatJet','SelectedFatJet*','SelectedFatJet_globalParT3*'] 
a.GetActiveNode().Snapshot(out_vars,output_file,'Events',lazy=False,openOption='RECREATE')

cutflow_histogram = ROOT.TH1F("h_cutflow", "Cutflow;Cut;Events", len(cutflow_labels), 0.5, len(cutflow_labels) + 0.5)
for bin_index, cut_label in enumerate(cutflow_labels, start=1):
    cutflow_histogram.GetXaxis().SetBinLabel(bin_index, cut_label)
    cutflow_histogram.SetBinContent(bin_index, cutflow_counts[bin_index - 1])

output_root_file = ROOT.TFile(output_file, "UPDATE")
output_root_file.cd()
cutflow_histogram.Write("", ROOT.TObject.kOverwrite)
output_root_file.Close()