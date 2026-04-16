timber_exec() {
    singularity exec \
        --bind `readlink $HOME` \
        --bind `readlink -f ${HOME}/nobackup/` \
        --bind /uscms_data/ \
        --bind /cvmfs \
        /cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/jhu-tools/timber:run3/ \
        python3 "$@"
}