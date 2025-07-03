#!/usr/bin/env python3
"""
Simple batch submission system for jet-selection processing
"""

import os
import json
import subprocess
import sys
from pathlib import Path
import zipfile
import glob

CHUNK_SIZE = 30  # Number of files per job chunk

#https://cmsweb.cern.ch/das/request?view=list&limit=50&instance=prod%2Fphys03&input=%2F*%2Flpcpfnano-Run3Summer22EEMiniAODv4-130X_mcRun3_2022_realistic_postEE_v6-v2_DAZSLE*0%2FUSER
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

def create_job_script_for_chunk(chunk_file, job_id):
    """Create job script that loops over the input/output pairs in chunk file."""
    script_content = f"""#!/bin/bash

echo "Starting job {job_id} at $(date)"
export SCRAM_ARCH=el9_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh

if [ -z "$JOBID" ]; then
  echo "ERROR: JOBID environment variable not set!"
  exit 1
fi

CHUNK_FILE="${{JOBID}}.txt"

echo "Unzipping input.zip..."
unzip -o input.zip
if [ $? -ne 0 ]; then
    echo "Failed to unzip input.zip"
    exit 1
fi

echo "ls after unzip"
ls

export CMSSW_VERSION=CMSSW_13_2_10
scram project CMSSW ${{CMSSW_VERSION}}
cd ${{CMSSW_VERSION}}/src
eval `scram runtime -sh`
cd ../..

python3 -m virtualenv timber-env 
source timber-env/bin/activate
cd TIMBER
source setup_alternative.sh
cd ..

echo "PWD after TIMBER setup"
pwd

echo "ls after TIMBER setup"
ls

while read input_file output_file; do
    echo "Processing input ${{input_file}} -> output ${{output_file}}"
    
    LOCAL_INPUT="input_local.root"
    xrdcp "root://cmseos.fnal.gov/${{input_file}}" "$LOCAL_INPUT"
    if [ $? -ne 0 ]; then
        echo "Failed to copy input file ${{input_file}}"
        exit 1
    fi

    SELECTED_FILE="selected_events.root"
    python selection.py --input "$LOCAL_INPUT" --output "$SELECTED_FILE"
    if [ $? -ne 0 ]; then
        echo "Selection failed for ${{input_file}}"
        exit 1
    fi

    LOCAL_OUTPUT="local.h5"
    python root_to_h5.py --input "$SELECTED_FILE" --output "$LOCAL_OUTPUT"
    if [ $? -ne 0 ]; then
        echo "root_to_h5 failed for ${{input_file}}"
        exit 1
    fi

    xrdcp "$LOCAL_OUTPUT" "root://cmseos.fnal.gov/${{output_file}}"
    if [ $? -ne 0 ]; then
        echo "Failed to copy output file ${{output_file}}"
        exit 1
    fi

    rm -f $SELECTED_FILE $LOCAL_INPUT $LOCAL_OUTPUT

done < $CHUNK_FILE

echo "Job {job_id} completed at $(date)"
"""
    script_path = Path(f"job_{job_id}.sh")
    with open(script_path, 'w') as f:
        f.write(script_content)
    os.chmod(script_path, 0o755)
    return script_path

def create_condor_jdl(job_scripts, dataset_name):
    """Create Condor JDL file with a single queue JOBID in (...) for all jobs."""
    job_ids = list(job_scripts.keys())

    jdl_content = f"""# Condor JDL for {dataset_name}

universe = vanilla
executable = $(script_name)
output = logs/job_$(JOBID).out
error = logs/job_$(JOBID).err
log = logs/job_$(JOBID).log

should_transfer_files = YES
transfer_input_files = input.zip

requirements = (OpSysAndVer =?= "AlmaLinux9")
request_cpus = 1
request_memory = 4GB
request_disk = 4GB

+JobFlavour = "workday"

"""

    # Define script_name variables
    for job_id, script_path in job_scripts.items():
        jdl_content += f'script_name_{job_id} = {script_path}\n'

    # Define script_name based on JOBID
    jdl_content += "\n"
    jdl_content += "def script_name = \\\n"
    jdl_content += "    " + " : ".join(
        [f'(JOBID == "{job_id}") ? script_name_{job_id}' for job_id in job_ids]
    ) + " : script_name_default\n\n"

    # Define JOBID list and queue
    jdl_content += "JOBID = " + " ".join(job_ids) + "\n"
    jdl_content += "queue JOBID in (" + " ".join(job_ids) + ")\n"

    jdl_path = Path(f"submit_{dataset_name}.jdl")
    with open(jdl_path, 'w') as f:
        f.write(jdl_content)

    return jdl_path


def main():
    if len(sys.argv) != 2:
        print("Usage: python submit_jobs.py config.json")
        sys.exit(1)
    
    config_file = sys.argv[1]
    
    # Load configuration
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)
    
    total_jobs = 0
    jobs_to_submit = 0
    
    # Process each dataset
    for dataset_info in config['datasets']:
        dataset = dataset_info['name']
        dataset_name = dataset.split('/')[1]  # Extract primary dataset name
        
        print(f"\nProcessing dataset: {dataset}")
        print(f"Dataset name: {dataset_name}")
        
        # Get files from DAS
        files = get_das_files(dataset)
        if not files:
            print(f"No files found for {dataset}")
            continue
        
        total_jobs += len(files)
        
        job_scripts = {}
        chunked_files = list(chunk_files(files))
        print(f"Splitting {len(files)} files into {len(chunked_files)} chunks of {CHUNK_SIZE} files")

        for chunk_id, file_chunk in enumerate(chunked_files):
            input_files = file_chunk
            output_files = [f"{config['output_dir']}/{dataset_name}/{get_output_filename(f)}" for f in input_files]

            # Filter out already processed files in this chunk
            filtered_pairs = [(inp, outp) for inp, outp in zip(input_files, output_files) if not check_output_exists(outp)]
            if not filtered_pairs:
                print(f"All files in chunk {chunk_id} already processed, skipping chunk.")
                continue

            filtered_input_files, filtered_output_files = zip(*filtered_pairs)
            chunk_file = create_input_list_file(filtered_input_files, filtered_output_files, dataset_name, chunk_id)
            job_id = f"{dataset_name}_chunk_{chunk_id:03d}"
            script_path = create_job_script_for_chunk(chunk_file, job_id)
            job_scripts[job_id] = script_path
            jobs_to_submit += len(filtered_input_files)

        if job_scripts:
            print(f"Creating {len(job_scripts)} jobs for {dataset_name}")
            jdl_path = create_condor_jdl(job_scripts, dataset_name)
            print(f"Submitting jobs with: condor_submit {jdl_path}")
            # For safety during testing, submission is disabled:
            # Uncomment below to enable actual submission
            # try:
            #     result = subprocess.run(["condor_submit", str(jdl_path)], 
            #                           capture_output=True, text=True, check=True)
            #     print("Jobs submitted successfully!")
            #     print(result.stdout)
            # except subprocess.CalledProcessError as e:
            #     print(f"Error submitting jobs: {e}")
            #     print(f"stderr: {e.stderr}")
        else:
            print(f"No jobs needed for {dataset_name}")
    
    print(f"\nSummary:")
    print(f"Total files: {total_jobs}")
    print(f"Jobs to submit: {jobs_to_submit}")
    print(f"Already processed: {total_jobs - jobs_to_submit}")

    print("Creating input.zip")
    create_input_zip()

def create_input_zip():
    with zipfile.ZipFile('input.zip', 'w') as zipf:
        # Glob patterns
        patterns = ["../*.py", "./*.txt"]
        for pattern in patterns:
            for filepath in glob.glob(pattern):
                zipf.write(filepath, arcname=os.path.basename(filepath))


if __name__ == "__main__":
    main()
