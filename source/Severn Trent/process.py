#!/usr/bin/env python3

import os
import pandas as pd
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from flood_processors.base_processor import BaseFloodProcessor

class SevernTrentProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Severn Trent')
        self.source_dir = Path(__file__).parent

    def process(self):
        """Process all Severn Trent data files and combine results."""
        dfs = []
        
        # Process EIR 793 datafile.xlsx (2010-2020)
        eir793_path = self.source_dir / "EIR 793 datafile.xlsx"
        if eir793_path.exists():
            dfs.extend(self._process_eir793(eir793_path))
            
        # Process EIR674 Flooding Data 2021 2023.xlsx
        eir674_path = self.source_dir / "EIR674 Flooding Data 2021 2023.xlsx"
        if eir674_path.exists():
            dfs.extend(self._process_eir674(eir674_path))
            
        # Process EIR641 2023 Flooding report data.xlsx
        eir641_path = self.source_dir / "EIR641 2023 Flooding report data.xlsx"
        if eir641_path.exists():
            dfs.append(self._process_eir641(eir641_path))
            
        if not dfs:
            raise ValueError("No data files found to process")
            
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Process each row and add to standardized data
        for _, row in combined_df.iterrows():
            # Standardize date
            incident_date = self.standardize_date(row['incident_date'])
            
            # Get location information
            location = {}
            if pd.notna(row.get('postcode')):
                location['postcode'] = row['postcode']
            if pd.notna(row.get('location')):
                location['town'] = row['location']
                
            # Add record
            self.add_record(
                incident_date=incident_date,
                incident_type=row['incident_type'],
                location=location
            )
        
        # Save the result
        self.save_results()
        
        return combined_df

    def _process_eir793(self, file_path):
        """Process EIR 793 datafile.xlsx which contains data from 2010-2020."""
        dfs = []
        
        # Process each year sheet
        for sheet_name in ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Rename columns to match standard format
            df = df.rename(columns={
                'Incident Date': 'incident_date',
                'Internal, External, Public Sewer Flooding, Public Area, Field': 'flooding_type',
                'Post Code': 'postcode',
                'Incident Cause': 'cause'
            })
            
            # Clean up flooding type
            df['flooding_type'] = df['flooding_type'].astype(str).str.strip()
            
            # Create incident type description
            df['incident_type'] = df['cause'].astype(str).str.strip()
            
            dfs.append(df)
            
        return dfs

    def _process_eir674(self, file_path):
        """Process EIR674 Flooding Data 2021 2023.xlsx."""
        dfs = []
        
        # Process each year sheet
        for sheet_name in ['2021', '2022', '2023']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Drop unnamed columns
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df = df.loc[:, df.columns != ' ']
            
            # Rename columns to match standard format
            df = df.rename(columns={
                'Incident Date': 'incident_date',
                'Internal/ External/ Public Sewer Flooding/Public Area/Field': 'flooding_type',
                'Location': 'location',
                'Incident Cause': 'cause'
            })
            
            # Clean up flooding type
            df['flooding_type'] = df['flooding_type'].astype(str).str.strip()
            
            # Create incident type description
            df['incident_type'] = df['cause'].astype(str).str.strip()
            
            dfs.append(df)
            
        return dfs

    def _process_eir641(self, file_path):
        """Process EIR641 2023 Flooding report data.xlsx."""
        df = pd.read_excel(file_path, sheet_name='Sheet1')
        
        # Rename columns to match standard format
        df = df.rename(columns={
            'Type': 'flooding_type',
            'Post Code': 'postcode'
        })
        
        # Add missing columns
        df['incident_date'] = None  # No date information in this file
        df['incident_type'] = df['flooding_type'].astype(str).str.strip()
        
        return df

if __name__ == "__main__":
    processor = SevernTrentProcessor()
    processor.process() 