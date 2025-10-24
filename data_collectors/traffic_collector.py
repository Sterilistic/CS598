import requests
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger(__name__)

class TrafficCollector:
    def __init__(self):
        self.api_key = Config.HERE_API_KEY
        self.base_url = Config.HERE_TRAFFIC_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EV-Analytics/1.0'
        })
    
    def get_traffic_flow(self, latitude: float, longitude: float, 
                        radius: float = 5.0) -> Optional[Dict]:
        """Get traffic flow data for a specific location"""
        try:
            params = {
                'apiKey': self.api_key,
                'bbox': self._create_bbox(latitude, longitude, radius),
                'responseattributes': 'sh,fc'
            }
            
            # Add rate limiting for free tier
            time.sleep(2.0)  # 2 second delay between requests
            
            response = self.session.get(self.base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            traffic_data = self._parse_traffic_data(data, latitude, longitude)
            
            logger.info(f"Successfully collected traffic data for {latitude}, {longitude}")
            return traffic_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch traffic data: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_traffic_flow: {str(e)}")
            return None
    
    def get_traffic_incidents(self, latitude: float, longitude: float, 
                            radius: float = 10.0) -> List[Dict]:
        """Get traffic incidents for a specific location"""
        try:
            # HERE Traffic API for incidents
            incidents_url = "https://traffic.ls.hereapi.com/traffic/6.3/incidents.json"
            
            params = {
                'apiKey': self.api_key,
                'bbox': self._create_bbox(latitude, longitude, radius),
                'responseattributes': 'sh,fc'
            }
            
            response = self.session.get(incidents_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            incidents = self._parse_incidents_data(data, latitude, longitude)
            
            logger.info(f"Successfully collected {len(incidents)} traffic incidents")
            return incidents
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch traffic incidents: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_traffic_incidents: {str(e)}")
            return []
    
    def _create_bbox(self, latitude: float, longitude: float, radius: float) -> str:
        """Create bounding box for API request"""
        # Convert radius from km to degrees (approximate)
        lat_delta = radius / 111.0  # 1 degree latitude ≈ 111 km
        lon_delta = radius / (111.0 * abs(latitude) / 90.0)  # Adjust for longitude
        
        north = latitude + lat_delta
        south = latitude - lat_delta
        east = longitude + lon_delta
        west = longitude - lon_delta
        
        return f"{south},{west},{north},{east}"
    
    def _parse_traffic_data(self, raw_data: Dict, latitude: float, longitude: float) -> Optional[Dict]:
        """Parse traffic flow data from API response"""
        try:
            flow_items = raw_data.get('RWS', [{}])[0].get('RW', [])
            
            if not flow_items:
                return None
            
            # Calculate average traffic metrics
            total_speed = 0
            total_flow = 0
            count = 0
            congestion_levels = []
            
            for flow_item in flow_items:
                fisd = flow_item.get('FIS', [{}])[0].get('FI', [])
                
                for fi in fisd:
                    # Extract traffic flow information
                    tmc = fi.get('TMC', {})
                    cf = fi.get('CF', [])
                    
                    for cf_item in cf:
                        speed = cf_item.get('SP', 0)
                        flow = cf_item.get('JF', 0)
                        
                        if speed > 0 and flow > 0:
                            total_speed += speed
                            total_flow += flow
                            count += 1
                            
                            # Determine congestion level
                            if speed < 20:
                                congestion_levels.append('severe')
                            elif speed < 40:
                                congestion_levels.append('moderate')
                            else:
                                congestion_levels.append('light')
            
            if count == 0:
                return None
            
            avg_speed = total_speed / count
            avg_flow = total_flow / count
            
            # Determine overall congestion level
            if congestion_levels.count('severe') > len(congestion_levels) * 0.5:
                congestion_level = 'severe'
            elif congestion_levels.count('moderate') > len(congestion_levels) * 0.3:
                congestion_level = 'moderate'
            else:
                congestion_level = 'light'
            
            traffic_data = {
                'timestamp': datetime.now(),
                'latitude': latitude,
                'longitude': longitude,
                'traffic_density': avg_flow,
                'average_speed_kmh': avg_speed,
                'congestion_level': congestion_level,
                'road_type': 'highway',  # Default assumption
                'distance_to_station_km': 0.0,  # Will be calculated later
                'incident_count': 0  # Will be updated with incidents data
            }
            
            return traffic_data
            
        except Exception as e:
            logger.error(f"Error parsing traffic data: {str(e)}")
            return None
    
    def _parse_incidents_data(self, raw_data: Dict, latitude: float, longitude: float) -> List[Dict]:
        """Parse traffic incidents data from API response"""
        incidents = []
        
        try:
            incident_items = raw_data.get('TRAFFIC_ITEMS', {}).get('TRAFFIC_ITEM', [])
            
            for item in incident_items:
                incident = {
                    'timestamp': datetime.now(),
                    'latitude': latitude,
                    'longitude': longitude,
                    'incident_type': item.get('TRAFFIC_ITEM_TYPE_DESC', ''),
                    'description': item.get('TRAFFIC_ITEM_DESCRIPTION', [{}])[0].get('value', ''),
                    'severity': item.get('CRITICALITY', {}).get('ID', ''),
                    'location': item.get('LOCATION', {}).get('GEOLOC', {}).get('ORIGIN', {})
                }
                incidents.append(incident)
                
        except Exception as e:
            logger.error(f"Error parsing incidents data: {str(e)}")
        
        return incidents
    
    def collect_traffic_for_stations(self, stations: List[Dict]) -> List[Dict]:
        """Collect traffic data for multiple charging stations"""
        traffic_data = []
        
        for station in stations:
            try:
                lat = station.get('latitude')
                lon = station.get('longitude')
                
                if lat and lon:
                    # Get traffic flow data
                    flow_data = self.get_traffic_flow(lat, lon)
                    if flow_data:
                        flow_data['station_id'] = station.get('id')
                        traffic_data.append(flow_data)
                    
                    # Get traffic incidents
                    incidents = self.get_traffic_incidents(lat, lon)
                    for incident in incidents:
                        incident['station_id'] = station.get('id')
                        traffic_data.append(incident)
                    
                    # Add delay to respect free tier rate limits (1 call per 2 seconds)
                    time.sleep(2.0)
                    
            except Exception as e:
                logger.error(f"Error collecting traffic for station {station.get('id')}: {str(e)}")
                continue
        
        logger.info(f"Collected traffic data for {len(traffic_data)} stations")
        return traffic_data
