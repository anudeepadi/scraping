import requests
import json
import logging
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import pandas as pd
from pathlib import Path
import time
from typing import Dict, List
import asyncio
import aiohttp
from tqdm import tqdm

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
        
        self.hierarchy_cache = {}

    async def fetch_with_retry(self, session: aiohttp.ClientSession, url: str, data: Dict, 
                             max_retries: int = 3) -> Dict:
        """Fetch data with retry logic"""
        for retry in range(max_retries):
            try:
                async with session.post(url, data=data) as response:
                    if response.status == 200:
                        return await response.json()
                    await asyncio.sleep(2 ** retry)
            except Exception as e:
                if retry == max_retries - 1:
                    raise
                await asyncio.sleep(2 ** retry)
        return None

    async def get_warehouses(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Fetch list of all warehouses"""
        cache_key = 'warehouses'
        if cache_key in self.hierarchy_cache:
            return self.hierarchy_cache[cache_key]
            
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getWHList",
            "Year": "2025",
            "Month": "01"
        }
        
        result = await self.fetch_with_retry(session, url, data)
        self.hierarchy_cache[cache_key] = result
        return result or []

    async def get_districts(self, session: aiohttp.ClientSession, warehouse_id: str = "All") -> List[Dict]:
        """Fetch list of districts for a warehouse"""
        cache_key = f'districts_{warehouse_id}'
        if cache_key in self.hierarchy_cache:
            return self.hierarchy_cache[cache_key]
            
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getDistrictList",
            "Year": "2025",
            "Month": "01",
            "WHId": warehouse_id
        }
        
        result = await self.fetch_with_retry(session, url, data)
        self.hierarchy_cache[cache_key] = result
        return result or []

    async def get_upazilas(self, session: aiohttp.ClientSession, warehouse_id: str = "All", 
                          district_id: str = "All") -> List[Dict]:
        """Fetch list of upazilas for a district"""
        cache_key = f'upazilas_{warehouse_id}_{district_id}'
        if cache_key in self.hierarchy_cache:
            return self.hierarchy_cache[cache_key]
            
        url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdplist/sdplist_Processing.php"
        data = {
            "operation": "getSDPUPList",
            "Year": "2025",
            "Month": "01",
            "gWRHId": warehouse_id,
            "gDistId": district_id
        }
        
        result = await self.fetch_with_retry(session, url, data)
        self.hierarchy_cache[cache_key] = result
        return result or []

    async def get_unions(self, session: aiohttp.ClientSession, upazila_id: str) -> List[Dict]:
        """Fetch list of unions for an upazila"""
        cache_key = f'unions_{upazila_id}'
        if cache_key in self.hierarchy_cache:
            return self.hierarchy_cache[cache_key]
            
        url = f"{self.base_url}/form2_view_datasource.php"
        data = {
            "operation": "getUnionList",
            "Year": "2025",
            "Month": "01",
            "UPNameList": upazila_id
        }
        
        result = await self.fetch_with_retry(session, url, data)
        self.hierarchy_cache[cache_key] = result
        return result or []

    def generate_date_ranges(self) -> List[Dict]:
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

    async def collect_hierarchy(self) -> List[Dict]:
        """Collect complete location hierarchy with progress bar"""
        async with aiohttp.ClientSession(headers=self.headers) as session:
            warehouse_data = []
            
            # Get warehouses
            warehouses = await self.get_warehouses(session)
            total_warehouses = len(warehouses)
            
            with tqdm(total=total_warehouses, desc="Collecting hierarchy") as pbar:
                for wh in warehouses:
                    wh_id = wh.get('id', 'All')
                    
                    # Get districts for this warehouse
                    districts = await self.get_districts(session, wh_id)
                    # Also include 'All' districts case
                    districts = [{'id': 'All', 'name': 'All'}] + districts
                    
                    for dist in districts:
                        dist_id = dist.get('id', 'All')
                        upazilas = await self.get_upazilas(session, wh_id, dist_id)
                        
                        for upz in upazilas:
                            upz_id = upz.get('upazila_id')
                            unions = await self.get_unions(session, upz_id)
                            
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
                    
                    pbar.update(1)
                    await asyncio.sleep(0.1)  # Rate limiting
            
            return warehouse_data

    async def save_metadata(self, output_dir="metadata"):
        """Save all metadata with hierarchy information"""
        Path(output_dir).mkdir(exist_ok=True)
        
        logger.info("Collecting location hierarchy...")
        hierarchy_data = await self.collect_hierarchy()
        
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
        
        # Save location hierarchy
        with open(f"{output_dir}/location_hierarchy.json", 'w', encoding='utf-8') as f:
            json.dump({
                "locations": hierarchy_data,
                "total_locations": len(hierarchy_data)
            }, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(hierarchy_data)} location combinations")
        
        # Create comprehensive combinations CSV
        csv_data = []
        for date_range in date_ranges:
            for location in hierarchy_data:
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
                        'item_name': item_name
                    })
        
        df = pd.DataFrame(csv_data)
        df.to_csv(f"{output_dir}/all_combinations.csv", index=False)
        
        # Save summary
        total_combinations = len(csv_data)
        summary = {
            "total_months": len(date_ranges),
            "total_warehouses": len(set(d['warehouse_id'] for d in hierarchy_data)),
            "total_districts": len(set(d['district_id'] for d in hierarchy_data)),
            "total_upazilas": len(set(d['upazila_id'] for d in hierarchy_data)),
            "total_unions": len(set(d['union_id'] for d in hierarchy_data)),
            "total_items": len(self.items),
            "total_combinations": total_combinations,
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

async def main():
    try:
        collector = LMISMetadataCollector()
        await collector.save_metadata()
    except Exception as e:
        logger.error(f"Metadata collection failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())