from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import logging
from flood_processors.base_processor import BaseFloodProcessor

class YorkshireWaterProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Yorkshire Water')
        self.data_dir = Path(__file__).parent

    def process_eir_937(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='Sheet1')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['Inc date'])
            incident_type = f"{row['Flooding source']} - {row['Int/Ext']} - {row['Curtilage/Non-Curtilage']}" if pd.notna(row['Flooding source']) and pd.notna(row['Int/Ext']) and pd.notna(row['Curtilage/Non-Curtilage']) else None
            location = self.create_location_dict(
                postcode=row['Postcode Prefix'] if pd.notna(row['Postcode Prefix']) else None,
                town=row['Town'] if pd.notna(row['Town']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_eir_996(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='EIR 966 Final')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['Inc Date'])
            incident_type = f"{row['Flooding Source']} - {row['Int/Ext/RTU']} - {row['Curtilage/Non Curtilage']}" if pd.notna(row['Flooding Source']) and pd.notna(row['Int/Ext/RTU']) and pd.notna(row['Curtilage/Non Curtilage']) else None
            location = self.create_location_dict(
                postcode=row['Postcode Prefix'] if pd.notna(row['Postcode Prefix']) else None,
                town=row['Town'] if pd.notna(row['Town']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process(self):
        file_937 = self.data_dir / 'EIR 937.xlsx'
        if file_937.exists():
            logging.info('Processing EIR 937.xlsx')
            self.process_eir_937(file_937)
        file_996 = self.data_dir / 'EIR 996.xlsx'
        if file_996.exists():
            logging.info('Processing EIR 996.xlsx')
            self.process_eir_996(file_996)
        self.save_results()

if __name__ == "__main__":
    processor = YorkshireWaterProcessor()
    processor.process() 