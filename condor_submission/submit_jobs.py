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
from contextlib import contextmanager

DEBUG=False

@contextmanager
def working_directory(path):
    """Context manager to temporarily change working directory"""
    original_dir = os.getcwd()
    os.makedirs(path, exist_ok=True)
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(original_dir)

def get_store_files(dataset):
    """Get list of files from EOS store path"""
    if DEBUG:
        print(f"Listing files in EOS store path: {dataset}")
    cmd = f"xrdfsls {dataset}"
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        files = [line.split()[-1] for line in result.stdout.strip().split('\n') if line]
        files = [f for f in files if f.endswith(".root") and "25v1" not in f]
        files.sort()  # Ensure consistent ordering
        if DEBUG:
            print(f"Found {len(files)} valid files (skipped any with '25v1')")
        return files
    except Exception as e:
        print(f"Error listing EOS store path: {e}")
        return []

def get_das_files(dataset):
    """Get list of files from DAS"""
    if DEBUG:
        print(f"Querying DAS for files in: {dataset}")
        
    cmd = f'dasgoclient --query="file dataset={dataset} instance=prod/phys03"'

    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, check=True)
        files = result.stdout.strip().split('\n') if result.stdout else []
        files = [f for f in files if "25v1" not in f]
        files.sort()  # Ensure consistent ordering
        if DEBUG:
            print(f"Found {len(files)} valid files (skipped any with '25v1')")
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

def chunk_files(files, chunk_size=10):
    """Yield successive chunks from files list."""
    for i in range(0, len(files), chunk_size):
        yield files[i:i + chunk_size]

def create_input_list_file(input_files, output_files, dataset_name, chunk_id):
    """Create a text file listing input/output pairs for a chunk."""
    chunk_filename = Path(f"{dataset_name}_chunk_{chunk_id:03d}.txt")
    with open(chunk_filename, 'w') as f:
        for in_f, out_f in zip(input_files, output_files):
            f.write(f"{in_f} {out_f} {dataset_name}\n")
    return chunk_filename

def create_job_script():
    """Create a generic job.sh script that takes chunk txt filename as first argument"""
    script_content = """#!/bin/bash
set -x
echo $TIMBERPATH

if [ -z "$1" ]; then
  echo "Usage: $0 <chunk_file.txt>"
  exit 1
fi

CHUNK_FILE="$1"

echo "Starting job processing chunk file ${CHUNK_FILE} at $(date)"

echo "Unzipping input.zip..."
unzip -o input.zip
if [ $? -ne 0 ]; then
    echo "Failed to unzip input.zip"
    exit 1
fi

echo "ls after unzip"
ls

while read input_file output_file process_name; do
    echo "Processing input ${input_file} -> output ${output_file}"

    LOCAL_INPUT="input_local.root"
    xrdcp "root://cmseos.fnal.gov/${input_file}" "${LOCAL_INPUT}"
    if [ $? -ne 0 ]; then
        echo "Failed to copy input file ${input_file}"
        exit 1
    fi

    SELECTED_FILE="selected_events.root"
    python3 selection.py "${LOCAL_INPUT}" "${SELECTED_FILE}" "${process_name}"

    if [ $? -ne 0 ]; then
        echo "Selection failed for ${input_file}"
        exit 1
    fi

    LOCAL_OUTPUT="local.h5"
    python3 root_to_h5.py "${SELECTED_FILE}" "${LOCAL_OUTPUT}"
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
transfer_output_files = ""

output = logs/job_$(args).out
error = logs/job_$(args).err
log = logs/job_$(args).log
+SingularityImage = "/cvmfs/unpacked.cern.ch/registry.hub.docker.com/mrogulji/timber:run3/"
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
    if DEBUG:
        print(f"Created Condor JDL file: {jdl_path} with ARGFILE: {argfile_name}")
    return jdl_path

def create_input_zip():
    """Zip all python scripts and txt files as input.zip"""
    with zipfile.ZipFile('input.zip', 'w') as zipf:
        patterns = ["../../*.py", "./*.txt"]
        for pattern in patterns:
            for filepath in glob.glob(pattern):
                zipf.write(filepath, arcname=os.path.basename(filepath))

        # Add TIMBER_modules directory recursively
        for foldername, subfolders, filenames in os.walk("../../TIMBER_modules"):
            for filename in filenames:
                filepath = os.path.join(foldername, filename)
                arcname = os.path.join("TIMBER_modules", os.path.relpath(filepath, "../../TIMBER_modules"))
                zipf.write(filepath, arcname=arcname)
    
    #print("Created input.zip with all .py, .txt, and TIMBER_modules files")

def merged_file_exists(output_dir, dataset_name):
    """Check if merged .h5 file exists in EOS output directory."""
    merged_file = f"{output_dir}/{dataset_name}.h5"
    print("DEBUG: Checking for merged file at:", merged_file)
    return check_output_exists(merged_file)

def main():
    if len(sys.argv) < 2:
        print("Usage: python submit_jobs.py config.json ")
        print("Optional for automatic submission: python submit_jobs.py config.json 1")
        sys.exit(1)
    
    config_file = sys.argv[1]
    with open(config_file, 'r') as f:
        config = json.load(f)
    if(len(sys.argv)==3 and sys.argv[2]=="1"):
        submit_flag = True
    else: 
        submit_flag = False
    
    Path("logs").mkdir(exist_ok=True)

    total_files = 0
    files_to_process = 0

    cleanup_patterns = ["*chunk*txt", "*.jdl", "*.pem", "*args.txt"]
    for pattern in cleanup_patterns:
        for f in glob.glob(pattern):
            try:
                os.remove(f)
                print(f"Removed {f}")
            except Exception as e:
                print(f"Could not remove {f}: {e}")


    for dataset_info in config['datasets']:
        print(20*"--")
        chunk_size = dataset_info['chunk_size']
        dataset = dataset_info['daspath']
        dataset_name = dataset_info['name']

        print(f"\nProcessing dataset: {dataset}")
        print(f"Dataset name: {dataset_name}")
        
        # Check if merged file already exists
        if merged_file_exists(config['output_dir'], dataset_name):
            print(f"Merged file already exists for {dataset_name}, skipping.")
            total_files += 1  # Count as processed
            continue
        
        with working_directory(dataset_name):
            if dataset.startswith("/store"): #Not listed on das
                files = get_store_files(dataset)
            else:
                files = get_das_files(dataset)
            if not files:
                print(f"No files found for {dataset}, skipping.")
                continue

            total_files += len(files)

            chunked_files = list(chunk_files(files,chunk_size=chunk_size))
            max_chunks = dataset_info.get("max_chunks", None)
            if max_chunks is not None:
                chunked_files = chunked_files[:max_chunks]
                if DEBUG:
                    print("Limiting the total numer of chunks!")
            if DEBUG:
                print(f"Split {len(files)} files into {len(chunked_files)} chunks of up to {chunk_size} files")

            chunk_txt_files = []
            files_to_process_this_dataset = 0

            for chunk_id, file_chunk in enumerate(chunked_files):
                input_files = file_chunk
                output_files = [f"{config['output_dir']}/{dataset_name}/{get_output_filename(f)}" for f in input_files]

                # Filter out files already processed
                filtered_pairs = [(inp, outp) for inp, outp in zip(input_files, output_files) if not check_output_exists(outp)]
                if not filtered_pairs:
                    continue

                filtered_input_files, filtered_output_files = zip(*filtered_pairs)
                chunk_file = create_input_list_file(filtered_input_files, filtered_output_files, dataset_name, chunk_id)
                chunk_txt_files.append(chunk_file)
                files_to_process_this_dataset += len(filtered_input_files)
                files_to_process += len(filtered_input_files)

            if chunk_txt_files:
                print(f"Number of files to be processed for {dataset_name}: {files_to_process_this_dataset}")
                create_input_zip()
                create_job_script()
                jdl_path = create_condor_jdl(chunk_txt_files, dataset_name)
                if submit_flag:
                    cmd = f"condor_submit {jdl_path}"
                    print(cmd)
                    os.system(cmd)
                else:
                    print(f"\nTo submit jobs run:\ncondor_submit {jdl_path}")
            else:
                print("No new jobs to submit for this dataset.")
                # All files processed but merged file not created yet
                if files:
                    print(f"All files processed for {dataset_name}. Running merge_h5.py...")
                    merge_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dataset_manipulation/merge_h5.py"))
                    try:
                        subprocess.run(
                            [sys.executable, merge_script, "--dataset-prefix", dataset_name, "--output-dir", config['output_dir']],
                            check=True
                        )
                        print(f"merge_h5.py completed for {dataset_name}")
                    except subprocess.CalledProcessError as e:
                        print(f"merge_h5.py failed for {dataset_name}: {e}")

    print("\nSummary:")
    print(f"Total files found: {total_files}")
    print(f"Files to be processed (jobs to submit): {files_to_process}")
    print(f"Files already processed: {total_files - files_to_process}")

if __name__ == "__main__":
    # python submit_jobs.py config.json
    main()
