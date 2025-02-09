import requests
import json
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import time
from typing import Dict, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LMISMetadataCollector:
    def __init__(self):
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest"
        }
        
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

    def get_warehouses(self) -> List[Dict]:
        """Fetch list of all warehouses"""
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getWHList",
            "Year": "2025",
            "Month": "01"
        }
        
        try:
            response = requests.post(url, headers=self.headers, data=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching warehouse list: {str(e)}")
            return []

    def get_districts(self, warehouse_id: str = "All") -> List[Dict]:
        """Fetch list of districts for a warehouse"""
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getDistrictList",
            "Year": "2025",
            "Month": "01",
            "WHId": warehouse_id
        }
        
        try:
            response = requests.post(url, headers=self.headers, data=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching district list: {str(e)}")
            return []

    def get_upazilas(self, warehouse_id: str = "All", district_id: str = "All") -> List[Dict]:
        """Fetch list of upazilas for a district"""
        url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdplist/sdplist_Processing.php"
        data = {
            "operation": "getSDPUPList",
            "Year": "2025",
            "Month": "01",
            "gWRHId": warehouse_id,
            "gDistId": district_id
        }
        
        try:
            response = requests.post(url, headers=self.headers, data=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching upazila list: {str(e)}")
            return []

    def get_unions(self, upazila_id: str) -> List[Dict]:
        """Fetch list of unions for an upazila"""
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getUnionList",
            "Year": "2025",
            "Month": "01",
            "UPNameList": upazila_id
        }
        
        try:
            response = requests.post(url, headers=self.headers, data=data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching union list: {str(e)}")
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
        
        # Fetch and save warehouse hierarchy
        warehouses = self.get_warehouses()
        warehouse_data = []
        
        for wh in warehouses:
            wh_id = wh.get('id', 'All')
            districts = self.get_districts(wh_id)
            
            for dist in districts:
                dist_id = dist.get('id', 'All')
                upazilas = self.get_upazilas(wh_id, dist_id)
                
                for upz in upazilas:
                    upz_id = upz.get('upazila_id')
                    unions = self.get_unions(upz_id)
                    
                    for union in unions:
                        warehouse_data.append({
                            'warehouse_id': wh_id,
                            'warehouse_name': wh.get('name', 'All'),
                            'district_id': dist_id,
                            'district_name': dist.get('name', 'All'),
                            'upazila_id': upz_id,
                            'upazila_name': upz.get('upazila_name'),
                            'union_id': union.get('id'),
                            'union_name': union.get('name')
                        })
                    
                    time.sleep(0.1)  # Rate limiting
        
        # Save hierarchy data
        with open(f"{output_dir}/location_hierarchy.json", 'w', encoding='utf-8') as f:
            json.dump({
                "locations": warehouse_data,
                "total_combinations": len(warehouse_data)
            }, f, indent=2, ensure_ascii=False)
        
        # Create combinations CSV
        csv_data = []
        for date_range in date_ranges:
            for location in warehouse_data:
                for item_code, item_name in self.items.items():
                    csv_data.append({
                        'year': date_range['year'],
                        'month': date_range['month'],
                        'warehouse_id': location['warehouse_id'],
                        'warehouse_name': location['warehouse_name'],
                        'district_id': location['district_id'],
                        'district_name': location['district_name'],
                        'upazila_id': location['upazila_id'],
                        'upazila_name': location['upazila_name'],
                        'union_id': location['union_id'],
                        'union_name': location['union_name'],
                        'item_code': item_code,
                        'item_name': item_name,
                        'file_name': f"{date_range['year']}_{date_range['month']}_{location['warehouse_id']}_{location['district_id']}_{location['upazila_id']}_{location['union_id']}_{item_code}.json"
                    })
        
        df = pd.DataFrame(csv_data)
        df.to_csv(f"{output_dir}/all_combinations.csv", index=False)
        
        # Save summary
        total_combinations = len(csv_data)
        summary = {
            "total_months": len(date_ranges),
            "total_warehouses": len(warehouses),
            "total_location_combinations": len(warehouse_data),
            "total_items": len(self.items),
            "total_data_combinations": total_combinations,
            "estimated_files": total_combinations,
            "date_generated": datetime.now().isoformat(),
            "date_range": {
                "start": "2016-11",
                "end": "2025-01"
            }
        }
        
        with open(f"{output_dir}/scraping_summary.json", 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2)
            
        logger.info(f"Total combinations to scrape: {total_combinations}")
        logger.info("Metadata collection completed!")

def main():
    collector = LMISMetadataCollector()
    collector.save_metadata()

if __name__ == "__main__":
    main()