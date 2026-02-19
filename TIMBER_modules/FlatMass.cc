#include "../include/common.h"
#include "ROOT/RVec.hxx"
#include "TH1F.h"

#ifndef M_PI
#define M_PI 3.14159
#endif

using namespace ROOT::VecOps;

class MassFlattener {
// This class provides a method to select jets based on flat mass criteria.
public:
    MassFlattener(float mass_max = 250.) : mass_max(mass_max), histogram("jet_histogram", "Jet Mass Histogram", static_cast<int>(mass_max / 5), 0, mass_max) {
    }

    RVec<int> SelectJetsFlatMass(RVec<float> FatJet_pt, RVec<float> FatJet_eta, RVec<float> FatJet_msoftdrop, float ptCut, float etaCut, float massCut, int max_count) {
        RVec<int> selectedJets;
        for (size_t i = 0; i < FatJet_pt.size(); ++i) {
            if (FatJet_pt[i] > ptCut && std::abs(FatJet_eta[i]) < etaCut && FatJet_msoftdrop[i] > massCut) {
                float mass = FatJet_msoftdrop[i];
                int bin = histogram.FindBin(mass);
                
                if (bin <= histogram.GetNbinsX() && histogram.GetBinContent(bin) >= max_count) {
                    continue; // Skip if bin count exceeds max_count, jets with mass > mass_max will be accepted as we assume they will have lower counts due to the falling spectrum
                }
                
                selectedJets.push_back(i);
                histogram.Fill(mass);
            }
        }
        return selectedJets;
    }

    void SaveHistogram(const std::string& filename) {
        TFile file(filename.c_str(), "RECREATE");
        histogram.Write();
        file.Close();
    }

private:
    float mass_max; // Preferably divisible with 5 because of assumed binning size of 5 GeV
    TH1F histogram;
};