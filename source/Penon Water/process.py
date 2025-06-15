#!/usr/bin/env python3

import os
import pandas as pd
import sys
from pathlib import Path
# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))
from flood_processors.base_processor import BaseFloodProcessor

class PenonWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Penon Water')
        self.source_dir = Path(__file__).parent
        self.company_name = "Penon Water"

    def process(self):
        """Process all Penon Water data files and combine results."""
        dfs = []
        
        # Process EIR25077.xlsx
        eir25077_path = self.source_dir / "EIR25077.xlsx"
        if eir25077_path.exists():
            dfs.extend(self._process_eir25077(eir25077_path))
            
        # Process EIR24187.xlsx
        eir24187_path = self.source_dir / "EIR24187.xlsx"
        if eir24187_path.exists():
            dfs.append(self._process_eir24187(eir24187_path))
            
        if not dfs:
            raise ValueError("No data files found to process")

            
        # Combine all DataFrames
        combined_df = pd.concat(dfs, ignore_index=True)
        

        for _, row in combined_df.iterrows():
            self.add_record(
                incident_date=self.standardize_date(row['incident_date']),
                incident_type=row['cause'],
                location={ 'type': 'postcode', 'value': row['postcode'] }
            )   
        
        # Save the result
        self.save_results()
        
        return combined_df

    def _process_eir25077(self, file_path):
        """Process EIR25077.xlsx file which contains both internal and external flooding data."""
        dfs = []
        
        # Process both sheets
        for sheet_name in ['External Sewer Floodings2010-23', 'Internal Sewer Floodings2010-23']:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
            
            # Clean up column names
            df.columns = [col.strip() if isinstance(col, str) else col for col in df.columns]
            
            # Drop unnamed columns
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            # Rename columns to match standard format
            df = df.rename(columns={
                'Post Code': 'postcode',
                'Raised Date': 'incident_date',
                'Flooding Cause': 'cause',
                'Location of Flooding': 'location'
            })

            # legend for cause
            cause_legend = {
                'BLPR': 'Blockage paper rag',
                'BLFT': 'Blockage fat',
                'BLST': 'Blockage silt',
                'BLDB': 'Blockage non sewage debris',
                'BLRT': 'Blockage roots',
                'CLBU': 'Collapse/burst',
                'PACB': 'Partial collapse',
                'PTCB': 'Partial collapse',
                'EQFL': 'Equipment failure',
                'HYOL': 'Hydraulic overload',
                'HOPS': 'Hydraulically overloaded pumping station',
                'SEWC': 'Sewer condition',
                'TPDM': 'Third party damage',
                'PSBL': 'Pump station blockage',
                'PSBR': 'Pump station breakdown'
            }

            df['cause'] = df['cause'].map(cause_legend)
            
            dfs.append(df)
            
        return dfs

    def _process_eir24187(self, file_path):
        """Process EIR24187.xlsx file."""
        df = pd.read_excel(file_path, sheet_name='Data')
        
        # Rename columns to match standard format
        df = df.rename(columns={
            'Date Raised': 'incident_date',
            'Town/City': 'location',
            'Postcode': 'postcode',
            'Flooding Category': 'flooding_type',
            'Feedback Responsibility': 'responsibility',
            'Feedback Cause': 'cause'
        })
        
        # Convert flooding type to lowercase
        df['flooding_type'] = df['flooding_type'].str.lower()
        
        return df

if __name__ == "__main__":
    processor = PenonWaterProcessor()
    processor.process() 