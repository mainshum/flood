import pandas as pd
from pathlib import Path
import argparse
import sys

def concatenate_parquet_files():
    """Reuse the concatenation logic from preview_results.py"""
    results_path = Path('results')
    parquet_files = list(results_path.glob('*.parquet'))
    
    if not parquet_files:
        print("No parquet files found in results folder", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(parquet_files)} parquet files in results folder:")
    for file in parquet_files:
        print(f"  - {file.name}")
    print()
    
    # Read and concatenate all parquet files
    dfs = []
    for file in parquet_files:
        df_temp = pd.read_parquet(file)
        dfs.append(df_temp)
    
    df = pd.concat(dfs, ignore_index=True)
    print(f"Concatenated {len(df)} total rows from {len(parquet_files)} files")
    print()
    
    return df

def main():
    parser = argparse.ArgumentParser(description='Generate incident type counts table from all parquet files in results folder')
    parser.add_argument('--output', '-o', help='Output file path (optional - if not provided, prints to console)')
    args = parser.parse_args()
    
    try:
        # Concatenate all parquet files
        df = concatenate_parquet_files()
        
        # Check if incident_type column exists
        if 'incident_type' not in df.columns:
            print("Error: 'incident_type' column not found in the data", file=sys.stderr)
            print("Available columns:", df.columns.tolist(), file=sys.stderr)
            sys.exit(1)
        
        # Generate incident type counts
        incident_counts = df['incident_type'].value_counts()
        
        # Create a formatted table
        print("=== INCIDENT TYPE COUNTS ===")
        print(f"Total incidents: {len(df)}")
        print(f"Unique incident types: {len(incident_counts)}")
        print()
        
        # Create a DataFrame for better formatting
        counts_df = pd.DataFrame({
            'Incident Type': incident_counts.index,
            'Count': incident_counts.values,
            'Percentage': (incident_counts.values / len(df) * 100).round(2)
        })
        
        # Display the table
        print(counts_df.to_string(index=False))
        
        # Save to file if output path provided
        if args.output:
            counts_df.to_csv(args.output, index=False)
            print(f"\nResults saved to: {args.output}")
            
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 