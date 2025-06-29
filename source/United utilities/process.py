from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import logging
from flood_processors.base_processor import BaseFloodProcessor
from datetime import datetime, timedelta

class UnitedUtilitiesProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('United Utilities')
        self.data_dir = Path(__file__).parent

    def excel_serial_to_date(self, serial):
        # Excel's serial date system starts at 1899-12-30
        try:
            return (datetime(1899, 12, 30) + timedelta(days=float(serial))).strftime('%Y-%m-%d')
        except Exception:
            return None

    def process_2023_flooding(self, file_path: Path):
        df = pd.read_excel(file_path, sheet_name='Flooding_2023')
        for _, row in df.iterrows():
            incident_date = self.standardize_date(row['INCIDENT DATE'])
            incident_type = f"{row['CATEGORY']} - {row['INCIDENT  CAUSE']}" if pd.notna(row['CATEGORY']) and pd.notna(row['INCIDENT  CAUSE']) else None
            location = self.create_location_dict(
                postcode=row['POSTCODE'] if pd.notna(row['POSTCODE']) else None
            )
            self.add_record(
                incident_date=incident_date,
                incident_type=incident_type,
                location=location
            )

    def process_xlsb(self, file_path: Path):
        for sheet in ['Internal', 'External']:
            df = pd.read_excel(file_path, sheet_name=sheet, engine='pyxlsb')
            for _, row in df.iterrows():
                incident_date = self.excel_serial_to_date(row['Incident Date'])
                incident_type = f"{row['Flooding Type']} - {row['Flooding Location']} - {row['Flooding Cause']}" if pd.notna(row['Flooding Type']) and pd.notna(row['Flooding Location']) and pd.notna(row['Flooding Cause']) else None
                location = self.create_location_dict(
                    postcode=row['Impacted Customer Postcode'] if pd.notna(row['Impacted Customer Postcode']) else None
                )
                self.add_record(
                    incident_date=incident_date,
                    incident_type=incident_type,
                    location=location
                )

    def process_2nd_request(self, file_path: Path):
        for sheet in ['FY21', 'FY22', 'FY23']:
            df = pd.read_excel(file_path, sheet_name=sheet)
            for _, row in df.iterrows():
                incident_date = self.standardize_date(row['Date'])
                incident_type = f"{row['Incident Type']} - {row['Cause']}" if pd.notna(row['Incident Type']) and pd.notna(row['Cause']) else None
                location = self.create_location_dict(
                    postcode=row['Part Postcode'] if pd.notna(row['Part Postcode']) else None
                )
                self.add_record(
                    incident_date=incident_date,
                    incident_type=incident_type,
                    location=location
                )

    def process(self):
        # 2023 Flooding
        file_2023 = self.data_dir / 'EIR 2023 Flooding.xlsx'
        if file_2023.exists():
            logging.info('Processing EIR 2023 Flooding.xlsx')
            self.process_2023_flooding(file_2023)
        # .xlsb file
        file_xlsb = self.data_dir / 'EIR-380 - Flooding Incidents Data.xlsb'
        if file_xlsb.exists():
            logging.info('Processing EIR-380 - Flooding Incidents Data.xlsb')
            self.process_xlsb(file_xlsb)
        # 2nd request
        file_2nd = self.data_dir / '2nd request' / 'EIR 260 Flooding Incidents Data.xlsx'
        if file_2nd.exists():
            logging.info('Processing 2nd request EIR 260 Flooding Incidents Data.xlsx')
            self.process_2nd_request(file_2nd)
        # Save results
        self.save_results()

if __name__ == "__main__":
    processor = UnitedUtilitiesProcessor()
    processor.process() 