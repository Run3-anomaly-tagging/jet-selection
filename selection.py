from TIMBER.Analyzer import *
from TIMBER.Tools.Common import *
import ROOT,sys

sys.path.append('../../')

if len(sys.argv) < 3:
    print("Usage: python selection.py input_file.root output_file.root")
    sys.exit(1)

input_file = sys.argv[1]
output_file = sys.argv[2]

# Import the C++
CompileCpp('TIMBER/Framework/include/common.h') # Compile (via gInterpreter) commonly used c++ code
CompileCpp('TIMBER/Framework/AnomalousJet_modules/JetMatchingToPDGID.cc')

# Create analyzer instance
a = analyzer(input_file)

ptCut = 300#GeV
etaCut = 2.4
massCut = 40

myCuts = CutGroup('myCuts')
myCuts.Add('njet',        'nFatJet>1') # NOTE: need to ensure two fat jets exist or next line will seg fault
myCuts.Add('pt_cut',      f'FatJet_pt[0] > {ptCut} && FatJet_pt[1] > {ptCut}')
myCuts.Add('eta_cut',     'abs(FatJet_eta[0]) < 2.4 && abs(FatJet_eta[1]) < 2.4')
myCuts.Add('mass_cut',     'abs(FatJet_msoftdrop[0]) > 40 && abs(FatJet_msoftdrop[1]) > 40')

myVars = VarGroup('myVars')
myVars.Add('lead_vector',       'hardware::TLvector(FatJet_pt[0],FatJet_eta[0],FatJet_phi[0],FatJet_msoftdrop[0])')
myVars.Add("lead_eta","FatJet_eta[0]")
myVars.Add("lead_phi","FatJet_phi[0]")
myVars.Add('sublead_vector',    'hardware::TLvector(FatJet_pt[1],FatJet_eta[1],FatJet_phi[1],FatJet_msoftdrop[1])')
myVars.Add('invariantMass',     'hardware::InvariantMass({lead_vector,sublead_vector})') 

a.Apply([myCuts,myVars])

a.Define("pfindices_lead_jet","GetPFCandIndicesForJet(FatJetPFCands_jetIdx,FatJetPFCands_pFCandsIdx,0,nFatJetPFCands)")
a.Define("pfindices_sublead_jet","GetPFCandIndicesForJet(FatJetPFCands_jetIdx,FatJetPFCands_pFCandsIdx,1,nFatJetPFCands)")

a.SubCollection("LeadPFCands", "PFCands", "pfindices_lead_jet",useTake=True, keep=["pt", "phi", "eta", "mass"])
a.SubCollection("SubleadPFCands", "PFCands", "pfindices_sublead_jet",useTake=True, keep=["pt", "phi", "eta", "mass"])

a.Define("idx0_vec", "ROOT::VecOps::RVec<int>{0}")
a.Define("idx1_vec", "ROOT::VecOps::RVec<int>{1}")
a.SubCollection("LeadFatJet", "FatJet",'idx0_vec',useTake=True, keep=["pt", "phi", "eta", "mass","globalParT3_hidNeuron"])
a.SubCollection("SubleadFatJet", "FatJet",'idx1_vec',useTake=True, keep=["pt", "phi", "eta", "mass","globalParT3_hidNeuron"])
#Sanity checks
#a.GetActiveNode().DataFrame.Display(['pfindices_lead_jet','pfindices_sublead_jet']).Print()
#a.GetActiveNode().DataFrame.Display(['LeadPFCands_eta','lead_eta','LeadPFCands_phi','lead_phi']).Print()

out_vars = ['nLeadPFCands','nSubleadPFCands','LeadPFCands*','SubleadPFCands*','nLeadFatJet','nSubleadFatJet','LeadFatJet*','SubleadFatJet*','SubleadFatJet_globalParT3*'] 
a.GetActiveNode().Snapshot(out_vars,output_file,'Events',lazy=False,openOption='RECREATE') 