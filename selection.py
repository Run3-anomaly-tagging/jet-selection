from TIMBER.Analyzer import *
from TIMBER.Tools.Common import *
import ROOT,sys

sys.path.append('../../')

if len(sys.argv) < 3:
    print("Usage: python selection.py input_file.root output_file.root optional:pdg_id_to_match")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

do_match = False
match_pdgid = 0
if len(sys.argv) > 3 and int(sys.argv[3])>0:
    match_pdgid = int(sys.argv[3])
    do_match = True


# Import the C++
CompileCpp('TIMBER/Framework/include/common.h') # Compile (via gInterpreter) commonly used c++ code
CompileCpp('TIMBER_modules/JetMatching.cc')

# Create analyzer instance
a = analyzer(input_file)

etaCut = 2.4

#Units in GeV
ptCut = 50
massCut = 30
pf_minpt = 1

myCuts = CutGroup('myCuts')
a.Cut('njet',        'nFatJet>0')
a.Cut('pt_cut',      f'FatJet_pt[0] > {ptCut}')

if do_match:
    a.Define("genmatch_selected_jet_indices",f"GenMatchSelectJets(FatJet_eta, FatJet_phi, GenPart_pdgId, GenPart_statusFlags, GenPart_eta, GenPart_phi, {match_pdgid}, 0.8)")
    a.Define("kin_passing_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut})")
    a.Define("selected_jet_indices", "IntersectIndices(genmatch_selected_jet_indices, kin_passing_jet_indices)")
else:
    a.Define("selected_jet_indices", f"SelectJets(FatJet_pt, FatJet_eta, FatJet_msoftdrop, {ptCut}, {etaCut}, {massCut})")



a.Cut("has_selected_jets", "selected_jet_indices.size() > 0")

a.Define("pfindices_selected_jet",f"GetPFCandIndicesForJets(FatJetPFCands_jetIdx,FatJetPFCands_pFCandsIdx,selected_jet_indices,nFatJetPFCands,FatJetPFCands_pt,{pf_minpt})")

a.SubCollection("SelectedPFCands", "PFCands", "pfindices_selected_jet",useTake=True, keep=["pt", "phi", "eta", "mass"])#,"jetIdx"])
a.Define("SelectedPFCands_jetMatchIdx","GetJetMatchIndexForPFCands(FatJetPFCands_jetIdx, FatJetPFCands_pFCandsIdx, selected_jet_indices, pfindices_selected_jet)")

a.SubCollection("SelectedFatJet", "FatJet",'selected_jet_indices',useTake=True, keep=["pt", "phi", "eta", "mass","globalParT3_hidNeuron"])

out_vars = ['nSelectedPFCands','nSelectedFatJet','SelectedPFCands*','nSelectedFatJet','SelectedFatJet*','SelectedFatJet_globalParT3*'] 
a.GetActiveNode().Snapshot(out_vars,output_file,'Events',lazy=False,openOption='RECREATE') 