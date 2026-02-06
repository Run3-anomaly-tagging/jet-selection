# Dataset Stitching

This script merges and stitches HDF5 files from multiple dataset samples with optional pT spectrum handling.

## Usage

```bash
python stitching.py [options]
```

### Options

- `--dataset-prefix`: Dataset prefix to match in config (default: `QCD`)
- `--merge-type`: Choose `flat` (pT binned), `realistic` (lumi matched), or `both` (default: `both`)
- `--output-dir`: Output directory for merged files (default: `../data`)
- `--config`: Path to config.json (default: `../condor_submission/config.json`)

### Examples

```bash
python stitching.py --dataset-prefix QCD --merge-type both
python stitching.py --dataset-prefix WJets --merge-type realistic
```