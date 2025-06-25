import pandas as pd
import numpy as np
from pathlib import Path
import re
from datetime import datetime
import logging
from typing import Dict, List, Optional, Tuple
import json

class BaseFloodProcessor:
    def __init__(self, company_name: str):
        self.company_name = company_name
        self.standardized_data = []
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'{company_name}_processing.log'),
                logging.StreamHandler()
            ]
        )
    
    def standardize_date(self, date_value) -> str:
        """
        Standardize date to YYYY-MM-DD format.
        """
        if pd.isna(date_value):
            return None
        try:
            if isinstance(date_value, str):
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d']:
                    try:
                        date_obj = datetime.strptime(date_value, fmt)
                        return date_obj.strftime('%Y-%m-%d')
                    except ValueError:
                        continue
                
                # Try year only
                if re.match(r'^\d{4}$', date_value):
                    return f"{date_value}-01-01"
                    
                # Try year and month
                if re.match(r'^\d{4}-\d{2}$', date_value):
                    return f"{date_value}-01"
                    
            elif isinstance(date_value, (pd.Timestamp, datetime)):
                return date_value.strftime('%Y-%m-%d')
                
        except Exception as e:
            logging.warning(f"Date standardization failed for value {date_value}: {str(e)}")
            return None
            
        return None

    def create_location_dict(self, **kwargs) -> Dict:
        """Create a standardized location dictionary."""
        return {
            'postcode': kwargs.get('postcode'),
            'town': kwargs.get('town'),
            'district': kwargs.get('district'),
            'county': kwargs.get('county')
        }

    def add_record(self, incident_date: str, incident_type: str, location: Dict) -> None:
        """Add a standardized record to the data list."""
        record = {
            'company': self.company_name,
            'incident_date': incident_date,
            'incident_type': incident_type,
            'location': location
        }
        
        self.standardized_data.append(record)

    def save_results(self, output_path: Optional[str] = None):
        """Save standardized data to parquet file."""
        if not self.standardized_data:
            logging.warning("No data to save!")
            return
            
        if output_path is None:
            output_path = f"results/{self.company_name}_incidents.parquet"
        else:
            output_path = f"results/{output_path}"
            
        # Ensure results directory exists
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
        df = pd.DataFrame(self.standardized_data)
        df.to_parquet(output_path, index=False)
        logging.info(f"Saved {len(df)} records to {output_path}")

    def process(self):
        """Main processing method to be implemented by each company."""
        raise NotImplementedError("Each company must implement its own process method") 