#include "../include/common.h"
#include "ROOT/RVec.hxx"

using namespace ROOT::VecOps; //RVec

// Match jet to gen particle by PDG ID within DeltaR
//Returns 1 if GenPart with ID is within DeltaR of the jet, 0 otherwise
int JetMatchingToPDGID(
    RVec<int> GenPart_pdgId, 
    RVec<int> GenPart_statusFlags, 
    RVec<ROOT::Math::PtEtaPhiMVector> GenPart_vect, 
    ROOT::Math::PtEtaPhiMVector jet, 
    int ID, 
    float R)
{
    for (size_t i = 0; i < GenPart_pdgId.size(); i++) {
        if (GenPart_statusFlags[i] & (1 << 13)) { // isLastCopy
            if (GenPart_pdgId[i] == ID) {
                if (hardware::DeltaR(GenPart_vect[i], jet) < R) {
                    return 1;
                }
            }
        }
    }
    return 0;
}

//Returns indices of PFCandidates that are associated with the selected jets and pass a minimum pt cut.
RVec<int> GetPFCandIndicesForJets(
    const RVec<int>& FatJetPFCands_jetIdx,
    const RVec<int>& FatJetPFCands_pFCandsIdx,
    const RVec<int>& selected_jet_indices,
    int nFatJetPFCands,
    const RVec<float>& FatJetPFCands_pt,
    const float PFCands_min_pt)
{
    RVec<int> result_indices;
    for (size_t i = 0; i < nFatJetPFCands; ++i) {
        if (Any(selected_jet_indices == FatJetPFCands_jetIdx[i]) && FatJetPFCands_pt[i]>PFCands_min_pt) {
            result_indices.push_back(FatJetPFCands_pFCandsIdx[i]);
        }
    }
    return result_indices;
}


//Retrieves the neuron vector at the specified index from a collection of per-jet neural network outputs.
RVec<float> GetNeuronVectorByIndex(
    const RVec<RVec<float>>& all_neurons, 
    int index)
{
    if (index < 0 || index >= (int)all_neurons.size()) return {};
    return all_neurons[index];
}

//Returns indices of jets passing pt, eta, and mass cuts.
RVec<int> SelectJets(
    const ROOT::VecOps::RVec<float> &pt,
    const ROOT::VecOps::RVec<float> &eta,
    const ROOT::VecOps::RVec<float> &mass,
    float ptCut,
    float etaCut,
    float massCut)
{
    ROOT::VecOps::RVec<int> indices;
    for (size_t i = 0; i < pt.size(); ++i) {
        if (pt[i] > ptCut && std::abs(eta[i]) < etaCut && mass[i] > massCut)
            indices.push_back(i);
    }
    return indices;
}

//For each selected PFCandidate, returns the index of the selected jet it is associated with.
RVec<int> GetJetMatchIndexForPFCands(
    const RVec<int>& FatJetPFCands_jetIdx,
    const RVec<int>& FatJetPFCands_pFCandsIdx,
    const RVec<int>& selected_jet_indices,
    const RVec<int>& selected_pfcand_indices)
{
    RVec<int> jet_match_indices;

    for (int i = 0; i < selected_pfcand_indices.size(); ++i) {
        int pfcand_idx = selected_pfcand_indices[i];

        // Find the FatJetPFCands index corresponding to this PFCand
        for (int j = 0; j < FatJetPFCands_jetIdx.size(); ++j) {
            if (FatJetPFCands_pFCandsIdx[j] == pfcand_idx) {
                int jetIdx = FatJetPFCands_jetIdx[j];
                // Find position in selected_jet_indices
                for (int k = 0; k < selected_jet_indices.size(); ++k) {
                    if (selected_jet_indices[k] == jetIdx) {
                        jet_match_indices.push_back(k);
                        break;
                    }
                }
                break;
            }
        }
    }

    return jet_match_indices;
}
