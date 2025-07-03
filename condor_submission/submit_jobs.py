#!/usr/bin/env python3
"""
Simple batch submission system for jet-selection processing
"""

import os
import json
import subprocess
import sys
from pathlib import Path

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

def create_job_script(input_file, output_file, job_id):
    """Create job script for a single file"""
    script_content = f"""#!/bin/bash

# Job script for {input_file}
echo "Starting job {job_id} at $(date)"
echo "Input: {input_file}"
echo "Output: {output_file}"

# Set up environment
export SCRAM_ARCH=el9_amd64_gcc11
source /cvmfs/cms.cern.ch/cmsset_default.sh

# Set up CMSSW
export CMSSW_VERSION=CMSSW_13_2_10
scram project CMSSW ${{CMSSW_VERSION}}
cd ${{CMSSW_VERSION}}/src
eval `scram runtime -sh`

cd ../..
python3 -m virtualenv timber-env 
git clone git@github.com:JHU-Tools/TIMBER.git
cd TIMBER/
mkdir bin
cd bin
git clone git@github.com:fmtlib/fmt.git
cd ../..

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/cms.cern.ch/el8_amd64_gcc10/external/boost/1.78.0-0d68c45b1e2660f9d21f29f6d0dbe0a0/lib

source timber-env/bin/activate
cd TIMBER
source setup_alternative.sh
cd ..

# Copy input file locally
echo "Copying input file..."
LOCAL_INPUT="input_.root"
xrdcp "root://cmseos.fnal.gov/{input_file}" "$LOCAL_INPUT"
if [ $? -ne 0 ]; then
    echo "Failed to copy input file"
    exit 1
fi

# Run selection
echo "Running selection..."
SELECTED_FILE="selected_events.root"
python selection.py --input "$LOCAL_INPUT" --output "$SELECTED_FILE"
if [ $? -ne 0 ]; then
    echo "Selection failed"
    exit 1
fi

# Run root_to_h5
echo "Running root_to_h5..."
LOCAL_OUTPUT="local.h5"
python root_to_h5.py --input "$SELECTED_FILE" --output "$LOCAL_OUTPUT"
if [ $? -ne 0 ]; then
    echo "root_to_h5 failed"
    exit 1
fi

# Copy output to EOS
echo "Copying output to EOS..."
xrdcp "$LOCAL_OUTPUT" "root://cmseos.fnal.gov/{output_file}"
if [ $? -ne 0 ]; then
    echo "Failed to copy output"
    exit 1
fi

echo "Job completed successfully at $(date)"
"""
    
    script_path = Path(f"job_{job_id}.sh")
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    os.chmod(script_path, 0o755)
    return script_path

def create_condor_jdl(job_scripts, dataset_name):
    """Create Condor JDL file"""
    jdl_content = f"""# Condor JDL for {dataset_name}

universe = vanilla
executable = $(script_name)
arguments = 
output = logs/job_$(job_id).out
error = logs/job_$(job_id).err
log = logs/job_$(job_id).log

should_transfer_files = YES
when_to_transfer_output = ON_EXIT
transfer_input_files = ../selection.py, ../root_to_h5.py

requirements = (OpSysAndVer =?= "AlmaLinux9")
request_cpus = 1
request_memory = 4GB
request_disk = 10GB

+JobFlavour = "workday"

"""
    
    for job_id, script_path in job_scripts.items():
        jdl_content += f"script_name = {script_path}\n"
        jdl_content += f"job_id = {job_id}\n"
        jdl_content += "queue 1\n\n"
    
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
        
        # Check which files need processing
        job_scripts = {}
        for i, input_file in enumerate(files):
            output_filename = get_output_filename(input_file)
            output_path = f"{config['output_dir']}/{dataset_name}/{output_filename}"
            
            if check_output_exists(output_path):
                print(f"Output exists, skipping: {output_filename}")
                continue
            
            # Create job script
            job_id = f"{dataset_name}_{i:04d}"
            script_path = create_job_script(input_file, output_path, job_id)
            job_scripts[job_id] = script_path
            jobs_to_submit += 1
        
        if job_scripts:
            print(f"Creating {len(job_scripts)} jobs for {dataset_name}")
            
            # Create JDL file
            jdl_path = create_condor_jdl(job_scripts, dataset_name)
            
            # Submit jobs
            print(f"Submitting jobs with: condor_submit {jdl_path}")
            print("SKIPPING THAT FOR NOW!")
            exit()
            try:
                result = subprocess.run(["condor_submit", str(jdl_path)], 
                                      capture_output=True, text=True, check=True)
                print("Jobs submitted successfully!")
                print(result.stdout)
            except subprocess.CalledProcessError as e:
                print(f"Error submitting jobs: {e}")
                print(f"stderr: {e.stderr}")
        else:
            print(f"No jobs needed for {dataset_name}")
    
    print(f"\nSummary:")
    print(f"Total files: {total_jobs}")
    print(f"Jobs to submit: {jobs_to_submit}")
    print(f"Already processed: {total_jobs - jobs_to_submit}")

if __name__ == "__main__":
    main()