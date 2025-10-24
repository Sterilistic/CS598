import requests
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger(__name__)

class OpenChargeMapCollector:
    def __init__(self):
        self.base_url = Config.OPENCHARGEMAP_API_URL
        self.api_key = Config.OPENCHARGEMAP_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EV-Analytics/1.0'
        })
    
    def get_charging_stations(self, 
                           country_code: str = 'US',
                           max_results: int = 1000,
                           latitude: Optional[float] = None,
                           longitude: Optional[float] = None,
                           distance: Optional[float] = None) -> List[Dict]:
        """
        Collect charging station data from OpenChargeMap API
        
        Args:
            country_code: Country code to filter stations
            max_results: Maximum number of results to return
            latitude: Center latitude for geographic search
            longitude: Center longitude for geographic search
            distance: Search radius in kilometers
        """
        try:
            params = {
                'output': 'json',
                'maxresults': min(max_results, 1000),  # API limit
                'countrycode': country_code,
                'verbose': 'true',
                'includecomments': 'false',
                'key': self.api_key
            }
            
            if latitude and longitude:
                params['latitude'] = latitude
                params['longitude'] = longitude
                if distance:
                    params['distance'] = distance
            
            logger.info(f"Fetching charging stations from OpenChargeMap API...")
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            stations = []
            
            for item in data:
                station = self._parse_station_data(item)
                if station:
                    stations.append(station)
            
            logger.info(f"Successfully collected {len(stations)} charging stations")
            return stations
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch charging stations: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_charging_stations: {str(e)}")
            return []
    
    def _parse_station_data(self, raw_data: Dict) -> Optional[Dict]:
        """Parse raw station data from API response"""
        try:
            station = {
                'id': str(raw_data.get('ID', '')),
                'name': raw_data.get('AddressInfo', {}).get('Title', ''),
                'latitude': raw_data.get('AddressInfo', {}).get('Latitude', 0),
                'longitude': raw_data.get('AddressInfo', {}).get('Longitude', 0),
                'address': raw_data.get('AddressInfo', {}).get('AddressLine1', ''),
                'city': raw_data.get('AddressInfo', {}).get('Town', ''),
                'state': raw_data.get('AddressInfo', {}).get('StateOrProvince', ''),
                'country': raw_data.get('AddressInfo', {}).get('Country', {}).get('Title', ''),
                'operator': raw_data.get('OperatorInfo', {}).get('Title', ''),
                'network': raw_data.get('Network', {}).get('Title', ''),
                'status': self._get_status(raw_data),
                'access_type': raw_data.get('AccessComments', ''),
                'pricing_info': raw_data.get('UsageCost', ''),
                'amenities': self._get_amenities(raw_data),
                'charging_points': self._parse_charging_points(raw_data.get('Connections', []))
            }
            
            return station
            
        except Exception as e:
            logger.error(f"Error parsing station data: {str(e)}")
            return None
    
    def _get_status(self, raw_data: Dict) -> str:
        """Extract station status"""
        status_info = raw_data.get('StatusType', {})
        return status_info.get('Title', 'Unknown')
    
    def _get_amenities(self, raw_data: Dict) -> str:
        """Extract amenities information"""
        amenities = []
        if raw_data.get('UsageType', {}).get('IsAccessKeyRequired'):
            amenities.append('Access Key Required')
        if raw_data.get('UsageType', {}).get('IsMembershipRequired'):
            amenities.append('Membership Required')
        if raw_data.get('UsageType', {}).get('IsPayAtLocation'):
            amenities.append('Pay at Location')
        return ', '.join(amenities)
    
    def _parse_charging_points(self, connections: List[Dict]) -> List[Dict]:
        """Parse charging point data"""
        points = []
        for conn in connections:
            point = {
                'id': f"{conn.get('ID', '')}_{conn.get('ConnectionType', {}).get('ID', '')}",
                'connector_type': conn.get('ConnectionType', {}).get('Title', ''),
                'power_kw': conn.get('PowerKW', 0),
                'voltage': conn.get('Voltage', 0),
                'amperage': conn.get('Amperage', 0),
                'status': conn.get('StatusType', {}).get('Title', 'Unknown'),
                'last_updated': datetime.now().isoformat()
            }
            points.append(point)
        return points
    
    def get_station_status_updates(self, station_ids: List[str]) -> List[Dict]:
        """
        Get real-time status updates for specific stations
        Note: This is a placeholder as OpenChargeMap doesn't provide real-time status
        """
        logger.info("OpenChargeMap doesn't provide real-time status updates")
        return []
    
    def collect_historical_usage(self, station_id: str, days_back: int = 30) -> List[Dict]:
        """
        Collect historical usage data
        Note: This is a placeholder as OpenChargeMap doesn't provide usage data
        """
        logger.info("OpenChargeMap doesn't provide historical usage data")
        return []
