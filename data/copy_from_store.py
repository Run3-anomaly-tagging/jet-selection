import os
import subprocess

path = "/store/user/roguljic/anomaly-tagging/H5_files/5Feb26/2022_postEE"
local_dir = "./"

def main():
    # List all .h5 files in the remote EOS directory
    eos_cmd = ["eos", "root://cmseos.fnal.gov", "ls", path]
    result = subprocess.run(eos_cmd, capture_output=True, text=True, check=True)
    files = [f for f in result.stdout.strip().split('\n') if f.endswith('.h5')]
    print(f"Found {len(files)} .h5 files in {path}")

    for fname in files:
        remote = f"root://cmseos.fnal.gov/{path}/{fname}"
        local = os.path.join(local_dir, fname)
        print(f"Copying {remote} -> {local}")
        os.system(f"xrdcp {remote} {local}")

if __name__ == "__main__":
    main()

