#include "../include/common.h"
#include "ROOT/RVec.hxx"

using namespace ROOT::VecOps; //RVec

int nBJets(RVec<float> scores, float wp) {
    int count = 0;
    int jetCount = 0;
    for (auto score : scores) {
        if (jetCount >= 4) break; // Only consider the four leading jets
        jetCount++;
        if (score > wp) count++;
    }
    return count;
}

int tagElectronIndex(const RVec<float>& ele_pt, const RVec<float>& ele_eta, const RVec<bool> ele_id, float ptCut, float etaCut) {
    for (size_t i = 0; i < ele_pt.size(); ++i) {
        if (ele_pt[i] > ptCut && std::abs(ele_eta[i]) < etaCut && ele_id[i]) {
            return i; // Return the index of the first electron that passes the cuts
        }
    }
    return -1; // Return -1 if no electron passes the cuts
}

int tagMuonIndex(const RVec<float>& mu_pt, const RVec<float>& mu_eta, const RVec<int>& mu_isoid, float ptCut, float etaCut, int iso_cut) {
    for (size_t i = 0; i < mu_pt.size(); ++i) {
        if (mu_pt[i] > ptCut && std::abs(mu_eta[i]) < etaCut && mu_isoid[i] >= iso_cut ) {
            return i; // Return the index of the first muon that passes the cuts
        }
    }
    return -1; // Return -1 if no muon passes the cuts
}

int nVetoElectrons(const RVec<float>& ele_pt, const RVec<float>& ele_eta, const RVec<bool>& ele_veto, float ptCut, float etaCut) {
    int count = 0;
    for (size_t i = 0; i < ele_pt.size(); ++i) {
        if (ele_pt[i] > ptCut && std::abs(ele_eta[i]) < etaCut && ele_veto[i]) {
            count++;
        }
    }
    return count;
}

int nVetoMuons(const RVec<float>& mu_pt, const RVec<float>& mu_eta, const RVec<int>&  mu_isoid, float ptCut, float etaCut, int iso_cut) {
    int count = 0;
    for (size_t i = 0; i < mu_pt.size(); ++i) {
        if (mu_pt[i] > ptCut && std::abs(mu_eta[i]) < etaCut && mu_isoid[i] >= iso_cut) {
            count++;
        }
    }
    return count;
}