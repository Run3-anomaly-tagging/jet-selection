#include "../include/common.h"
#include "ROOT/RVec.hxx"

#ifndef M_PI
#define M_PI 3.14159
#endif

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

float DeltaPhi(float phi1, float phi2){
    float dPhi = std::fabs(phi1 - phi2);
    if (dPhi > M_PI) dPhi = 2 * M_PI - dPhi;
    return dPhi;
}

RVec<int> GenMatchSelectJets(
    const RVec<float>& jet_eta,
    const RVec<float>& jet_phi,
    const RVec<int>& gen_pdgId,
    const RVec<int>& gen_statusFlags,
    const RVec<float>& gen_eta,
    const RVec<float>& gen_phi,
    int matchID,
    float dR)
{
    std::vector<std::pair<float, float>> matchedGenCoords;
    float dR2 = dR*dR;
    // First: select isolated gen particles
    for (size_t i = 0; i < gen_pdgId.size(); ++i) {
        if (!(gen_statusFlags[i] & (1 << 13))) continue; // isLastCopy
        if (std::abs(gen_pdgId[i]) != matchID) continue;

        float eta = gen_eta[i];
        float phi = gen_phi[i];
        bool is_isolated = true;

        for (const auto& [stored_eta, stored_phi] : matchedGenCoords) {
            float dEta = eta - stored_eta;
            float dPhi = DeltaPhi(phi,stored_phi);

            if ((dEta * dEta + dPhi * dPhi) < dR2) {
                is_isolated = false;
                break;
            }
        }

        if (is_isolated) {
            matchedGenCoords.emplace_back(eta, phi);
        }
    }

    // Second: match jets to gen particles
    RVec<int> matchedJetIndices;
    for (size_t j = 0; j < jet_eta.size(); ++j) {
        float j_eta = jet_eta[j];
        float j_phi = jet_phi[j];

        for (const auto& [g_eta, g_phi] : matchedGenCoords) {
            float dEta = j_eta - g_eta;
            float dPhi = DeltaPhi(j_phi,g_phi);

            if ((dEta * dEta + dPhi * dPhi) < dR2) {
                matchedJetIndices.push_back(j);
                break;
            }
        }
    }

    return matchedJetIndices;
}


//Returns indices that appear in both lists
RVec<int> IntersectIndices(RVec<int> a, RVec<int> b) {
    std::unordered_set<int> b_set(b.begin(), b.end());
    RVec<int> out;
    for (auto i : a)
        if (b_set.count(i)) out.push_back(i);
    return out;
}


RVec<int> TruncateIndices(const RVec<int>& indices, size_t maxN) {
    if (indices.size() <= maxN) return indices;
    return RVec<int>(indices.begin(), indices.begin() + maxN);
}