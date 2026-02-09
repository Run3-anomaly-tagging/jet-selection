## Dataset Stitching

This script merges and stitches HDF5 files from multiple dataset samples with optional pT spectrum handling.

### Usage

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

## Scaling Factor Calculation

The `calculate_scaling.py` script computes mean and std.dev. per feature to be used in training for standardization of the features.

### Usage

```bash
python calculate_scaling.py --input input_file.h5 --output output.npz --max-jets 1000000
```