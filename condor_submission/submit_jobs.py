#!/usr/bin/env python3
"""
Simple batch submission system for jet-selection processing
Uses ARGFILE approach for Condor submission with chunk txt filenames as arguments.
"""

import os
import json
import subprocess
import sys
from pathlib import Path
import zipfile
import glob

CHUNK_SIZE = 30  # Number of files per job chunk

def get_das_files(dataset):
    """Get list of files from DAS dataset"""
    print(f"Querying DAS for files in: {dataset}")
        
    cmd = f'dasgoclient --query="file dataset={dataset} instance=prod/phys03"'

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        files = result.stdout.strip().split('\n') if result.stdout else []
        print(f"Found {len(files)} files")
        return files
    except Exception as e:
        print(f"Error querying DAS: {e}")
        return []

def check_output_exists(output_path):
    """Check if output file exists on EOS"""
    try:
        result = subprocess.run(["xrdfs", "root://cmseos.fnal.gov", "stat", output_path], 
                              capture_output=True, text=True)
        return result.returncode == 0
    except:
        return False

def get_output_filename(input_file):
    """Generate output filename from input file"""
    return Path(input_file).stem + ".h5"

def chunk_files(files, chunk_size=CHUNK_SIZE):
    """Yield successive chunks from files list."""
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

def create_input_list_file(input_files, output_files, dataset_name, chunk_id):
    """Create a text file listing input/output pairs for a chunk."""
    chunk_filename = Path(f"{dataset_name}_chunk_{chunk_id:03d}.txt")
    with open(chunk_filename, 'w') as f:
        for in_f, out_f in zip(input_files, output_files):
            f.write(f"{in_f} {out_f}\n")
    return chunk_filename
def create_job_script():
    """Create a generic job.sh script that takes chunk txt filename as first argument"""
    script_content = """#!/bin/bash
set -x

if [ -z "$1" ]; then
  echo "Usage: $0 <chunk_file.txt>"
  exit 1
fi

CHUNK_FILE="$1"

echo "Starting job processing chunk file ${CHUNK_FILE} at $(date)"
export SCRAM_ARCH=el9_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh

echo "Unzipping input.zip..."
unzip -o input.zip
if [ $? -ne 0 ]; then
    echo "Failed to unzip input.zip"
    exit 1
fi

echo "ls after unzip"
ls

xrdcp root://cmseos.fnal.gov//store/user/roguljic/anomaly-tagging/timber_tar.tgz ./
tar -xf timber_tar.tgz
rm timber_tar.tgz
echo "----------------"
ls
echo "----------------"


export CMSSW_VERSION=CMSSW_13_2_10
scram project CMSSW ${CMSSW_VERSION}
cd ${CMSSW_VERSION}
eval `scram runtime -sh`
cd ..

python3 -m virtualenv timber-env 
source timber-env/bin/activate

cd TIMBER
make clean
export BOOST_BASE=/cvmfs/cms.cern.ch/el9_amd64_gcc11/external/boost/1.78.0-c49033d06e1a3bf1beac1c01e1ef27d6/
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$BOOST_BASE/lib
export CPLUS_INCLUDE_PATH=$BOOST_BASE/include:$CPLUS_INCLUDE_PATH

echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
echo "CPLUS_INCLUDE_PATH: $CPLUS_INCLUDE_PATH"

echo "STARTING TIMBER setup"
source setup.sh
cd ..

echo "PWD after TIMBER setup"
pwd

echo "----------------"
echo "ls after TIMBER setup"
ls
echo "----------------"

while read input_file output_file; do
    echo "Processing input ${input_file} -> output ${output_file}"
    
    LOCAL_INPUT="input_local.root"
    xrdcp "root://cmseos.fnal.gov/${input_file}" "${LOCAL_INPUT}"
    if [ $? -ne 0 ]; then
        echo "Failed to copy input file ${input_file}"
        exit 1
    fi

    SELECTED_FILE="selected_events.root"
    python selection.py --input "${LOCAL_INPUT}" --output "${SELECTED_FILE}"
    if [ $? -ne 0 ]; then
        echo "Selection failed for ${input_file}"
        exit 1
    fi

    LOCAL_OUTPUT="local.h5"
    python root_to_h5.py --input "${SELECTED_FILE}" --output "${LOCAL_OUTPUT}"
    if [ $? -ne 0 ]; then
        echo "root_to_h5 failed for ${input_file}"
        exit 1
    fi

    xrdcp "${LOCAL_OUTPUT}" "root://cmseos.fnal.gov/${output_file}"
    if [ $? -ne 0 ]; then
        echo "Failed to copy output file ${output_file}"
        exit 1
    fi

    rm -f ${SELECTED_FILE} ${LOCAL_INPUT} ${LOCAL_OUTPUT}

done < "${CHUNK_FILE}"

echo "Job processing chunk ${CHUNK_FILE} completed at $(date)"
"""
    script_path = Path("job.sh")
    with open(script_path, 'w') as f:
        f.write(script_content)
    os.chmod(script_path, 0o755)
    print(f"Created job script: {script_path}")
    return script_path


def create_condor_jdl(chunk_files_list, dataset_name):
    """Create Condor JDL file using ARGFILE approach"""
    argfile_name = f"{dataset_name}_args.txt"
    with open(argfile_name, 'w') as f:
        for chunk_file in chunk_files_list:
            f.write(f"{chunk_file}\n")

    jdl_content = f"""# Condor JDL for {dataset_name}

universe = vanilla
executable = job.sh
arguments = $(args)
transfer_input_files = input.zip, job.sh
transfer_output_files = 

output = logs/job_$(args).out
error = logs/job_$(args).err
log = logs/job_$(args).log

requirements = (OpSysAndVer =?= "AlmaLinux9")
request_cpus = 1
request_memory = 4GB
request_disk = 4GB

+JobFlavour = "workday"

queue args from {argfile_name}
"""
    jdl_path = Path(f"submit_{dataset_name}.jdl")
    with open(jdl_path, 'w') as f:
        f.write(jdl_content)

    print(f"Created Condor JDL file: {jdl_path} with ARGFILE: {argfile_name}")
    return jdl_path

def create_input_zip():
    """Zip all python scripts and txt files as input.zip"""
    with zipfile.ZipFile('input.zip', 'w') as zipf:
        patterns = ["../*.py", "./*.txt"]
        for pattern in patterns:
            for filepath in glob.glob(pattern):
                zipf.write(filepath, arcname=os.path.basename(filepath))
    print("Created input.zip with all .py and .txt files")

def main():
    if len(sys.argv) != 2:
        print("Usage: python submit_jobs.py config.json")
        sys.exit(1)
    
    config_file = sys.argv[1]
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    Path("logs").mkdir(exist_ok=True)

    total_files = 0
    files_to_process = 0

    for dataset_info in config['datasets']:
        dataset = dataset_info['name']
        dataset_name = dataset.split('/')[1]  # extract primary dataset name
        
        print(f"\nProcessing dataset: {dataset}")
        print(f"Dataset name: {dataset_name}")

        files = get_das_files(dataset)
        if not files:
            print(f"No files found for {dataset}, skipping.")
            continue

        total_files += len(files)

        chunked_files = list(chunk_files(files))
        print(f"Split {len(files)} files into {len(chunked_files)} chunks of up to {CHUNK_SIZE} files")

        chunk_txt_files = []

        for chunk_id, file_chunk in enumerate(chunked_files):
            input_files = file_chunk
            output_files = [f"{config['output_dir']}/{dataset_name}/{get_output_filename(f)}" for f in input_files]

            # Filter out files already processed
            filtered_pairs = [(inp, outp) for inp, outp in zip(input_files, output_files) if not check_output_exists(outp)]
            if not filtered_pairs:
                print(f"All files in chunk {chunk_id} already processed, skipping chunk.")
                continue

            filtered_input_files, filtered_output_files = zip(*filtered_pairs)
            chunk_file = create_input_list_file(filtered_input_files, filtered_output_files, dataset_name, chunk_id)
            chunk_txt_files.append(chunk_file)
            files_to_process += len(filtered_input_files)

        if chunk_txt_files:
            create_input_zip()
            create_job_script()
            jdl_path = create_condor_jdl(chunk_txt_files, dataset_name)

            print(f"\nTo submit jobs run:\ncondor_submit {jdl_path}")
        else:
            print("No new jobs to submit for this dataset.")

    print("\nSummary:")
    print(f"Total files found: {total_files}")
    print(f"Files to be processed (jobs to submit): {files_to_process}")
    print(f"Files already processed: {total_files - files_to_process}")

if __name__ == "__main__":
    main()
