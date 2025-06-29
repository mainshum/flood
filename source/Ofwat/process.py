from pathlib import Path
import sys
import os

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

import logging
from flood_processors.base_processor import BaseFloodProcessor

class OfwatProcessor(BaseFloodProcessor):
    def __init__(self):
        super().__init__('Ofwat')
        self.data_dir = Path(__file__).parent

    def process(self):
        logging.info("Ofwat data contains aggregated summary statistics, not individual incident records.")
        logging.info("Skipping processing as per requirements to focus on granular incident-level data.")
        logging.info("Files contain: External/Internal sewer flooding aggregated by company and year.")
        # No data to save as this is aggregated information
        logging.info("No individual incident records extracted from Ofwat data.")

if __name__ == "__main__":
    processor = OfwatProcessor()
    processor.process() 