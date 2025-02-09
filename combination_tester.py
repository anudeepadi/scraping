import asyncio
import aiohttp
import json
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from typing import Dict, List, Set
import itertools

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/combination_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CombinationTester:
    def __init__(self):
        self.base_url = "https://elmis.dgfp.gov.bd/dgfplmis_reports/sdpdataviewer"
        self.headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "*/*",
            "X-Requested-With": "XMLHttpRequest"
        }
        
    async def fetch_data(self, session: aiohttp.ClientSession, params: Dict) -> Dict:
        """Fetch data with specific parameters"""
        url = f"{self.base_url}/form2_view_datasource.php"
        
        try:
            async with session.post(url, data=params) as response:
                if response.status == 200:
                    text = await response.text()
                    return {
                        'params': params,
                        'data': json.loads(text) if text else None,
                        'status': 'success'
                    }
                return {
                    'params': params,
                    'status': f'error-{response.status}',
                    'data': None
                }
        except Exception as e:
            return {
                'params': params,
                'status': f'error-{str(e)}',
                'data': None
            }

    async def test_combinations(self, sample_params: List[Dict]) -> Dict:
        """Test different parameter combinations"""
        results = {
            'timestamp': datetime.now().isoformat(),
            'tests': []
        }
        
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for params in sample_params:
                # Test with All vs Specific values
                test_cases = [
                    {'name': 'all_all', 'wh': 'All', 'dist': 'All'},
                    {'name': 'specific_all', 'wh': params['warehouse_id'], 'dist': 'All'},
                    {'name': 'all_specific', 'wh': 'All', 'dist': params['district_id']},
                    {'name': 'specific_specific', 'wh': params['warehouse_id'], 'dist': params['district_id']}
                ]
                
                test_results = []
                for case in test_cases:
                    api_params = {
                        "operation": "getItemlist",
                        "Year": params['year'],
                        "Month": params['month'],
                        "Item": params['item_code'],
                        "UPNameList": params['upazila_id'],
                        "UnionList": params['union_id'],
                        "WHListAll": case['wh'],
                        "DistrictList": case['dist']
                    }
                    
                    result = await self.fetch_data(session, api_params)
                    test_results.append({
                        'case': case['name'],
                        'result': result
                    })
                    
                    # Add delay between requests
                    await asyncio.sleep(1)
                
                results['tests'].append({
                    'params': params,
                    'test_results': test_results
                })
        
        return results

    def analyze_results(self, results: Dict) -> Dict:
        """Analyze test results to find data differences"""
        analysis = {
            'timestamp': datetime.now().isoformat(),
            'total_tests': len(results['tests']),
            'differences_found': 0,
            'differences': []
        }
        
        for test in results['tests']:
            base_data = None
            base_case = None
            differences = []
            
            for result in test['test_results']:
                if result['result']['status'] == 'success':
                    data = result['result']['data']
                    if base_data is None:
                        base_data = data
                        base_case = result['case']
                    else:
                        if self._compare_data(base_data, data):
                            differences.append({
                                'base_case': base_case,
                                'compare_case': result['case'],
                                'params': test['params']
                            })
            
            if differences:
                analysis['differences_found'] += 1
                analysis['differences'].extend(differences)
        
        return analysis

    def _compare_data(self, data1: Dict, data2: Dict) -> bool:
        """Compare two API responses, return True if different"""
        if not data1 or not data2:
            return True
            
        if 'aaData' not in data1 or 'aaData' not in data2:
            return True
            
        # Compare data length
        if len(data1['aaData']) != len(data2['aaData']):
            return True
            
        # Compare data content
        for row1, row2 in zip(data1['aaData'], data2['aaData']):
            if row1 != row2:
                return True
                
        return False

async def main():
    # Create test directory
    Path('test_results').mkdir(exist_ok=True)
    
    tester = CombinationTester()
    
    # Load sample parameters from metadata
    try:
        df = pd.read_csv('metadata/all_combinations.csv')
        # Take a sample of different combinations
        sample = df.sample(n=min(10, len(df)))
        
        # Convert sample to list of dicts
        sample_params = sample.to_dict('records')
        
        # Run tests
        logger.info("Starting combination tests...")
        results = await tester.test_combinations(sample_params)
        
        # Analyze results
        analysis = tester.analyze_results(results)
        
        # Save results
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        with open(f'test_results/combination_test_{timestamp}.json', 'w') as f:
            json.dump(results, f, indent=2)
            
        with open(f'test_results/analysis_{timestamp}.json', 'w') as f:
            json.dump(analysis, f, indent=2)
        
        logger.info(f"Tests completed. Found {analysis['differences_found']} combinations with differences.")
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())