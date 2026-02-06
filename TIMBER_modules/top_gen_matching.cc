#include <TFile.h>
#include <TMath.h>
#include <stdio.h>
#include <vector>
#include <iostream>
#include "ROOT/RVec.hxx"

typedef ROOT::VecOps::RVec<int> rvec_i;
typedef ROOT::VecOps::RVec<int> rvec_f;

Float_t deltaR(Float_t eta1, Float_t phi1, Float_t eta2, Float_t phi2);
Int_t classifyProbeJet(Int_t fatJetIdx,rvec_f FatJet_phi,rvec_f FatJet_eta, Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother);
Int_t bFromTopinJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother );
Int_t bFromTopBothinJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother );
Int_t qFromWInJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother );
Int_t qqFromWAllInJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother );
rvec_i classifyTopJets(rvec_f FatJet_phi, rvec_f FatJet_eta, rvec_i pfindices_selected_jet, Int_t nGenPart, rvec_f GenPart_phi, rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother);


Float_t deltaR(Float_t eta1, Float_t phi1, Float_t eta2, Float_t phi2) {
    Float_t dEta = eta1 - eta2;
    Float_t dPhi = phi1 - phi2;

    if (dPhi > TMath::Pi())       dPhi -= 2.0 * TMath::Pi();
    else if (dPhi <= -TMath::Pi()) dPhi += 2.0 * TMath::Pi();

    return TMath::Sqrt(dEta * dEta + dPhi * dPhi);
}


Int_t bFromTopinJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother ){
    for(Int_t i=0; i<nGenPart;i++){
        Int_t pid = GenPart_pdgId[i];
        Int_t motherIdx = GenPart_genPartIdxMother[i];
        Int_t motherPid = GenPart_pdgId[motherIdx];

        if(motherPid==-1){
            continue;
        }

        Float_t dR = deltaR(GenPart_eta[i],GenPart_phi[i],FatJet_eta,FatJet_phi);

        if(TMath::Abs(pid)==5 && TMath::Abs(motherPid)==6 && dR<0.8){
            return 1;
        }
    }
    return 0;
}

Int_t bFromTopBothinJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother ){
    for(Int_t i=0; i<nGenPart;i++){
        Int_t pid = GenPart_pdgId[i];
        Int_t motherIdx = GenPart_genPartIdxMother[i];
        Int_t motherPid = GenPart_pdgId[motherIdx];

        if(motherPid==-1){
            continue;
        }

        Float_t dR = deltaR(GenPart_eta[i],GenPart_phi[i],FatJet_eta,FatJet_phi);
        Float_t dRMother = deltaR(GenPart_eta[motherIdx],GenPart_phi[motherIdx],FatJet_eta,FatJet_phi);

        if(TMath::Abs(pid)==5 && TMath::Abs(motherPid)==6 && dR<0.8 && dRMother<0.8){
            return 1;
        }
    }
    return 0;
}

Int_t qFromWInJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother ){
    for(Int_t i=0; i<nGenPart;i++){
        Int_t pid = GenPart_pdgId[i];
        Int_t motherIdx = GenPart_genPartIdxMother[i];
        Int_t motherPid = GenPart_pdgId[motherIdx];

        if(motherPid==-1){
            continue;
        }

        Float_t dR = deltaR(GenPart_eta[i],GenPart_phi[i],FatJet_eta,FatJet_phi);
        if(dR<0.8 && TMath::Abs(pid)<6 && TMath::Abs(pid)>0 && TMath::Abs(motherPid)==24){
            return 1;
        }
    }
    return 0;
}

Int_t qqFromWAllInJet(Float_t FatJet_phi, Float_t FatJet_eta,Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother ){
    Int_t nQ = 0;
    Int_t isWInJet = 0;
    for(Int_t i=0; i<nGenPart;i++){
        Int_t pid = GenPart_pdgId[i];
        Int_t motherIdx = GenPart_genPartIdxMother[i];
        Int_t motherPid = GenPart_pdgId[motherIdx];

        if(motherPid==-1){
            continue;
        }

        Float_t dR = deltaR(GenPart_eta[i],GenPart_phi[i],FatJet_eta,FatJet_phi);
        Float_t dRMother = deltaR(GenPart_eta[motherIdx],GenPart_phi[motherIdx],FatJet_eta,FatJet_phi);
        if(dR<0.8 && TMath::Abs(pid)<6 && TMath::Abs(pid)>0 && TMath::Abs(motherPid)==24 && dRMother<0.8){
            nQ = nQ+1;
            isWInJet = isWInJet+1;
        }
    }
    if(nQ>1 && isWInJet>1){
        return 1;
    }
    else{
        return 0;
    }
}

Int_t classifyProbeJet(Int_t fatJetIdx,rvec_f FatJet_phi,rvec_f FatJet_eta, Int_t nGenPart, rvec_f GenPart_phi,rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother){
//1: qq, 2: bq, 3:bqq, 0 other
Int_t btInJet = bFromTopinJet(FatJet_phi[fatJetIdx],FatJet_eta[fatJetIdx],nGenPart,GenPart_phi,GenPart_eta,GenPart_pdgId,GenPart_genPartIdxMother);
Int_t bInJet = bFromTopBothinJet(FatJet_phi[fatJetIdx],FatJet_eta[fatJetIdx],nGenPart,GenPart_phi,GenPart_eta,GenPart_pdgId,GenPart_genPartIdxMother);
Int_t qInJet = qFromWInJet(FatJet_phi[fatJetIdx],FatJet_eta[fatJetIdx],nGenPart,GenPart_phi,GenPart_eta,GenPart_pdgId,GenPart_genPartIdxMother);
Int_t qqWInJet = qqFromWAllInJet(FatJet_phi[fatJetIdx],FatJet_eta[fatJetIdx],nGenPart,GenPart_phi,GenPart_eta,GenPart_pdgId,GenPart_genPartIdxMother);

    if(btInJet && qqWInJet){
        return 3;
    }
    else if(bInJet && qInJet){
        return 2;
    }
    else if(qqWInJet){
        return 1;
    }
    else{
        return 0;
    }
}


rvec_i classifyTopJets(rvec_f FatJet_phi, rvec_f FatJet_eta, rvec_i pfindices_selected_jet, Int_t nGenPart, rvec_f GenPart_phi, rvec_f GenPart_eta, rvec_i GenPart_pdgId, rvec_i GenPart_genPartIdxMother){
    // For each fat jet, assign a classification label.
    // - Jets listed in pfindices_selected_jet are passed to classifyProbeJet()
    //   to determine their label: unmatched, qq, b, bqq jets are labeled as 0,1,2,3
    // - All other jets are assigned a default value of -1.
    rvec_i result(FatJet_phi.size(), -1); // initialize with -1

    for (size_t i = 0; i < pfindices_selected_jet.size(); i++) {
        Int_t fatJetIdx = pfindices_selected_jet[i];
        if (fatJetIdx >= 0 && fatJetIdx < (Int_t)FatJet_phi.size()) {
            result[fatJetIdx] = classifyProbeJet(fatJetIdx,FatJet_phi,FatJet_eta,nGenPart,GenPart_phi,GenPart_eta,GenPart_pdgId,GenPart_genPartIdxMother);
        }
    }
    return result;
}
