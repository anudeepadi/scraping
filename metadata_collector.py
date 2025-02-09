import requests
import json
from pathlib import Path
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LMISMetadataCollector:
    def __init__(self):
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        
        # Define static item mappings based on the buttons
        self.items = {
            "CON008": "Shukhi",
            "CON010": "Shukhi (3rd Gen)",
            "CON008+CON010": "Oral Pill (Total)",
            "CON009": "Oral Pill Apon",
            "CON002": "Condom",
            "CON006": "Injectables (Vials)",
            "CON001": "AD Syringe (1ML)",
            "CON003": "ECP",
            "MCH021": "Tab. Misoprostol (Dose)",
            "MCH051": "7.1% CHLOROHEXIDINE",
            "MCH012": "MNP(SUSSET)",
            "MCH018": "Iron-Folic Acid (NOS)"
        }

    def get_upazila_list(self, year: str, month: str) -> list:
        """Fetch list of all upazilas"""
        url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdplist/sdplist_Processing.php"
        data = {
            "operation": "getSDPUPList",
            "Year": year,
            "Month": month,
            "gWRHId": "All",
            "gDistId": "All"
        }
        
        try:
            response = requests.post(url, headers=self.headers, data=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching upazila list: {str(e)}")
            return []

    def generate_date_ranges(self):
        """Generate all month-year combinations from Nov 2016 to Jan 2025"""
        start_date = date(2016, 11, 1)
        end_date = date(2025, 1, 31)
        
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append({
                'year': str(current_date.year),
                'month': str(current_date.month).zfill(2)
            })
            current_date += relativedelta(months=1)
        return dates

    def save_metadata(self, output_dir="metadata"):
        """Save all metadata required for full scraping"""
        Path(output_dir).mkdir(exist_ok=True)
        
        # Save item mappings
        with open(f"{output_dir}/item_mappings.json", 'w', encoding='utf-8') as f:
            json.dump({
                "items": [
                    {"code": code, "name": name} 
                    for code, name in self.items.items()
                ]
            }, f, indent=2, ensure_ascii=False)
        logger.info("Saved item mappings")
        
        # Generate and save date ranges
        date_ranges = self.generate_date_ranges()
        with open(f"{output_dir}/date_ranges.json", 'w', encoding='utf-8') as f:
            json.dump({
                "date_ranges": date_ranges,
                "total_months": len(date_ranges)
            }, f, indent=2)
        logger.info(f"Saved {len(date_ranges)} date ranges")
        
        # Fetch and save upazila list
        upazilas = self.get_upazila_list("2025", "01")
        
        # Save complete upazila details
        with open(f"{output_dir}/upazilas.json", 'w', encoding='utf-8') as f:
            json.dump({
                "upazilas": upazilas,
                "total_upazilas": len(upazilas)
            }, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(upazilas)} upazila details")
        
        # Create a summary of total combinations
        total_combinations = len(date_ranges) * len(upazilas) * len(self.items)
        summary = {
            "total_months": len(date_ranges),
            "total_upazilas": len(upazilas),
            "total_items": len(self.items),
            "total_combinations": total_combinations,
            "estimated_files": total_combinations,
            "date_generated": datetime.now().isoformat(),
            "date_range": {
                "start": f"2016-11",
                "end": f"2025-01"
            }
        }
        
        with open(f"{output_dir}/scraping_summary.json", 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Total combinations to scrape: {total_combinations}")
        
        # Create CSV mapping for easy reference
        csv_data = []
        for date_range in date_ranges:
            for upazila in upazilas:
                for item_code, item_name in self.items.items():
                    csv_data.append({
                        'year': date_range['year'],
                        'month': date_range['month'],
                        'upazila_id': upazila['upazila_id'],
                        'upazila_name': upazila['upazila_name'],
                        'item_code': item_code,
                        'item_name': item_name,
                        'file_name': f"{date_range['year']}_{date_range['month']}_{upazila['upazila_id']}_{item_code}.json"
                    })
        
        df = pd.DataFrame(csv_data)
        df.to_csv(f"{output_dir}/all_combinations.csv", index=False)
        logger.info("Saved CSV mapping of all combinations")

def main():
    collector = LMISMetadataCollector()
    collector.save_metadata()

if __name__ == "__main__":
    main()