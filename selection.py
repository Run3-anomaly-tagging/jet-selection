from TIMBER.Analyzer import *
from TIMBER.Tools.Common import *
import ROOT,sys

sys.path.append('../../')

default_cuts = {
    'pt_min':170.,
    'abs_eta_max':2.4,
    'mass_min':40.,
    'deltaR_max':0.8 #For DeltaR matching
}

process_to_id = {
    'TTto4Q': [0],
    'Hbb': [25],
    'Wqq': [24],
    'Zqq': [23],
    'QCD': [0],
    'Yto4q': [35],
    'Data': [0],
    'SVJ': [4900111,4900113,4900211,4900213],
    'EMJ' : [4900101,4900113,4900111,4900211,4900213]
}


def detect_qcd_flavour_mode(process_name):
    n = process_name.lower()
    if "qcd_b" in n:
        return "b"
    if "qcd_c" in n:
        return "c"
    if "qcd_light" in n:
        return "light"
    return None

def flavour_to_int(mode):
    if mode == "b":
        return 5
    if mode == "c":
        return 4
    if mode == "light":
        return 0
    return -1

def sanitize_process_name(process_name):
    n = process_name.lower()
    if "qcd" in n:
        return "QCD"
    elif "wjets" in n:
        return "Wqq"
    elif "zjets" in n:
        return "Zqq"
    elif "glugluhto2b" in n:
        return "Hbb"
    elif "jetmet" in n:
        return "Data"
    elif "svj" in n:
        return "SVJ"
    elif "emj" in n:
        return "EMJ"
    else:
        return process_name

if len(sys.argv) < 4:
    print("Usage: python selection.py input_file.root output_file.root process")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

flavour_mode = detect_qcd_flavour_mode(sys.argv[3])
target_hadron_flavour = flavour_to_int(flavour_mode)
process_name = sanitize_process_name(sys.argv[3])

match_pdgid_list = process_to_id.get(process_name)
if match_pdgid_list is None:
    print(f"Process name {process_name} not recognized.")
    sys.exit(1)

if match_pdgid_list != [0]:
    do_match = True
else:
    do_match = False


# Import the C++
CompileCpp('TIMBER/Framework/include/common.h')
CompileCpp('TIMBER_modules/JetMatching.cc')

# Create analyzer instance
a = analyzer(input_file)

etaCut = default_cuts["abs_eta_max"]
ptCut = default_cuts["pt_min"]
massCut = default_cuts["mass_min"]

myCuts = CutGroup('myCuts')
a.Cut('njet',        'nFatJet>0')
a.Cut('pt_cut',      f'FatJet_pt[0] > {ptCut}')#Ordered in pT so we can apply cut on first jet

if do_match:
    # Returns indices of jets matched to gen particles of given pdgid(s) within deltaR
    a.Define("genmatch_selected_jet_indices",
        f"GenMatchSelectJets(FatJet_eta, FatJet_phi, GenPart_pdgId, GenPart_statusFlags, GenPart_eta, GenPart_phi, std::vector<int>{{{','.join(str(x) for x in match_pdgid_list)}}}, 0.8)")
    # Returns indices of jets passing pt, eta, and mass cuts.
    a.Define("kin_passing_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut})")
    a.Define("base_selected_jet_indices", "IntersectIndices(genmatch_selected_jet_indices, kin_passing_jet_indices)")
else:
    a.Define("base_selected_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut})")

if flavour_mode is not None:
    a.Define(
        "selected_jet_indices",
        f"FilterJetsByHadronFlavour(base_selected_jet_indices, FatJet_hadronFlavour, {target_hadron_flavour})"
    )
else:
    a.Define("selected_jet_indices", "base_selected_jet_indices")


#Remove events without jets to avoid crashing
a.Cut("has_selected_jets", "selected_jet_indices.size() > 0")

#We keep only leading two jets after selection
a.Define("pruned_selected_jet_indices", "TruncateIndices(selected_jet_indices,2)")

keep_list = ["pt", "phi", "eta", "msoftdrop","globalParT3_hidNeuron","globalParT3_QCD","globalParT3_TopbWqq","globalParT3_TopbWq","hadronFlavour","particleNet_QCD"]

if (process_name=="TTto4Q"):
    CompileCpp('TIMBER_modules/top_gen_matching.cc')
    a.Define("FatJet_top_cat","classifyTopJets(FatJet_phi, FatJet_eta, pruned_selected_jet_indices, nGenPart, GenPart_phi, GenPart_eta, GenPart_pdgId, GenPart_genPartIdxMother)")
    keep_list.append("top_cat")

a.SubCollection("SelectedFatJet", "FatJet",'pruned_selected_jet_indices',useTake=True, keep=keep_list)

out_vars = ['nSelectedFatJet','SelectedFatJet*','SelectedFatJet_globalParT3*'] 
a.GetActiveNode().Snapshot(out_vars,output_file,'Events',lazy=False,openOption='RECREATE')