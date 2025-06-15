import pandas as pd
from pathlib import Path
import argparse
import sys

def main():
    parser = argparse.ArgumentParser(description='Preview first 10 rows of a parquet file')
    parser.add_argument('file', help='Path to the parquet file')
    args = parser.parse_args()
    
    try:
        df = pd.read_parquet(args.file)
        print(f"\nFirst 10 rows of {args.file}:")
        print(df.head(10))
    except Exception as e:
        print(f"Error reading {args.file}: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 