# jet-selection

Requires TIMBER: https://gitlab.cern.ch/jhu-tools/TIMBER
Container available in CVMFS: `/cvmfs/unpacked.cern.ch/registry.hub.docker.com/mrogulji/timber:run3/`

## Running on lpc

Add to .bashrc for ease-of-use 
```
timber_exec() {
    singularity exec \
        --bind "$(readlink $HOME)" \
        --bind "$(readlink -f ${HOME}/nobackup/)" \
        --bind /uscms_data \
        --bind /etc/grid-security/certificates \
        --bind /cvmfs \
        /cvmfs/unpacked.cern.ch/registry.hub.docker.com/mrogulji/timber:run3/ \
        python3 "$@"
}
```

Usage:
```
timber_exec selection.py <input.root> <selected_jets.root> <PDG_ID for matching (optional)>
timber_exec root_to_h5.py <selected_jets.root> <output.h5>
```

Testing
```
#ZJets
timber_exec selection.py /store/group/lpcpfnano/PFNano_Run3/25v2/sixie/mc_2022EE/Zto2Q-4Jets_HT-600to800_TuneCP5_13p6TeV_madgraphMLM-pythia8/Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_DAZSLE_PFNano/250619_153411/0000/MC_postEE2022_30.root Zqq_selected.root ZJets800
timber_exec root_to_h5.py Zqq_selected.root Zqq_test.h5

#SVJ
timber_exec selection.py SVJ.root SVJ_selected.root SVJ
timber_exec root_to_h5.py SVJ_selected.root SVJ_test.h5
```
