#include "../include/common.h"
#include "ROOT/RVec.hxx"

#ifndef M_PI
#define M_PI 3.14159
#endif

using namespace ROOT::VecOps; //RVec

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
    const std::vector<int>& matchIDs,
    float dR)
{
    float dR2 = dR*dR;

    // Collect all gen particles of interest (matching any of the provided PDG IDs)
    std::vector<std::pair<float, float>> genCoords;
    for (size_t i = 0; i < gen_pdgId.size(); ++i) {
        if (!(gen_statusFlags[i] & (1 << 13))) continue; // isLastCopy
        for (auto id : matchIDs) {
            if (std::abs(gen_pdgId[i]) == id) {
                genCoords.emplace_back(gen_eta[i], gen_phi[i]);
                break;
            }
        }
    }

    // Match jets to any gen particle within dR
    RVec<int> matchedJetIndices;
    for (size_t j = 0; j < jet_eta.size(); ++j) {
        float j_eta = jet_eta[j];
        float j_phi = jet_phi[j];

        for (const auto& [g_eta, g_phi] : genCoords) {
            float dEta = j_eta - g_eta;
            float dPhi = DeltaPhi(j_phi, g_phi);

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