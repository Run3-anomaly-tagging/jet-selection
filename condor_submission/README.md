## Batch Job Submission & Merging Workflow

This directory contains scripts for submitting jet-selection jobs to Condor and merging their outputs.

### 1. Dry Run (Prepare Jobs)

Generate all required files and see what would be submitted, without actually submitting jobs:

```bash
python submit_jobs.py config.json
```

This will:
- Check if merged files already exist (skip datasets that are done)
- List files from DAS/EOS for remaining datasets
- Split datasets into chunks
- Create job scripts and Condor JDL files
- Print submission commands (e.g., `condor_submit submit_QCD_HT-1000to1200.jdl`)

You can then manually submit jobs by running the printed `condor_submit` commands in the appropriate directories.

---

### 2. Automatic Submission

To automatically submit all jobs:

```bash
python submit_jobs.py config.json 1
```

This prepares jobs as above and automatically submits them to Condor.

---

### 3. Checking Status & Automatic Stitching

After jobs finish, re-run the submission script to check status:

```bash
python submit_jobs.py config.json
```

The script will:
- Skip datasets with existing merged files
- Check for missing individual output files
- If all files are processed and no merged file exists, automatically run `merge_h5_store.py` to merge individual `.h5` files into a single dataset file

---

### 4. Manual Merging (Legacy)

For directly merging files from an EOS path:

```bash
python merge_h5_store.py <eos_output_path> [dataset_name]
```

- `<eos_output_path>`: EOS directory containing dataset subdirectories with `.h5` files
- `[dataset_name]` (optional): Process only a single dataset; if omitted, processes all subdirectories