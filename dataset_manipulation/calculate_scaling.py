import h5py
import numpy as np
import argparse

def print_h5_structure(f, indent=0, max_depth=3):
    """Recursively print HDF5 file structure."""
    if indent > max_depth:
        return
    for key in f.keys():
        item = f[key]
        prefix = "  " * indent
        if isinstance(item, h5py.Dataset):
            print(f"{prefix}{key}: Dataset, shape={item.shape}, dtype={item.dtype}")
        elif isinstance(item, h5py.Group):
            print(f"{prefix}{key}: Group")
            print_h5_structure(item, indent + 1, max_depth)

def calculate_scaling(input_file, output_file, key="Jets", max_jets=None):
    """
    Calculate mean and std of hidNeurons features from HDF5 file.
    
    Args:
        input_file: Path to input HDF5 file
        output_file: Path to output .npz file
        key: HDF5 dataset key (default: "Jets")
        max_jets: Maximum number of jets to use for calculation (None = all)
    """
    print(f"Loading data from {input_file}")
    with h5py.File(input_file, 'r') as f:        
        jets = f[key]
        n_jets = len(jets) if max_jets is None else min(len(jets), max_jets)
        print(f"Using {n_jets} jets for scaling calculation")
        
        # Get feature dimension from first jet
        first_jet = jets[0]['hidNeurons'][:]
        n_features = len(first_jet)
        print(f"Number of features: {n_features}")
        
        # Calculate mean and std incrementally to avoid memory issues
        running_sum = np.zeros(n_features, dtype=np.float64)
        running_sum_sq = np.zeros(n_features, dtype=np.float64)
        
        batch_size = 10000
        n_batches = (n_jets + batch_size - 1) // batch_size
        
        for batch_idx in range(n_batches):
            start_idx = batch_idx * batch_size
            end_idx = min(start_idx + batch_size, n_jets)
            
            # Load batch
            batch_features = []
            for i in range(start_idx, end_idx):
                features = jets[i]['hidNeurons'][:].astype(np.float64)
                batch_features.append(features)
            
            batch_features = np.array(batch_features)
            
            # Update running statistics
            running_sum += batch_features.sum(axis=0)
            running_sum_sq += (batch_features ** 2).sum(axis=0)
            
            if (batch_idx + 1) % 10 == 0 or batch_idx == n_batches - 1:
                print(f"Processed {end_idx}/{n_jets} jets")
        
        # Calculate final mean and std
        mean = running_sum / n_jets
        variance = (running_sum_sq / n_jets) - (mean ** 2)
        std = np.sqrt(variance)
        
        print(f"\nStatistics:")
        print(f"Mean range: [{mean.min():.4f}, {mean.max():.4f}]")
        print(f"Std range: [{std.min():.4f}, {std.max():.4f}]")
        
        # Save to .npz file
        np.savez(output_file, mean=mean.astype(np.float32), std=std.astype(np.float32))
        print(f"\nScaling parameters saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Calculate mean and std for jet features")
    parser.add_argument("--input", type=str, required=True,
                       help="Input HDF5 file path")
    parser.add_argument("--output", type=str, required=True,
                       help="Output .npz file path")
    parser.add_argument("--key", type=str, default="Jets",
                       help="HDF5 dataset key (default: Jets)")
    parser.add_argument("--max-jets", type=int, default=None,
                       help="Maximum number of jets to use (default: all)")
    
    args = parser.parse_args()
    
    calculate_scaling(args.input, args.output, key=args.key, max_jets=args.max_jets)

if __name__ == "__main__":
    main()
