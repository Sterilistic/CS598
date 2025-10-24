import requests
import logging
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger(__name__)

class WeatherCollector:
    def __init__(self):
        self.api_key = Config.OPENWEATHER_API_KEY
        self.base_url = Config.OPENWEATHER_API_URL
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'EV-Analytics/1.0'
        })
    
    def get_current_weather(self, latitude: float, longitude: float) -> Optional[Dict]:
        """Get current weather data for a specific location"""
        try:
            params = {
                'lat': latitude,
                'lon': longitude,
                'appid': self.api_key,
                'units': 'metric'
            }
            
            response = self.session.get(f"{self.base_url}/weather", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            weather_data = self._parse_weather_data(data)
            
            logger.info(f"Successfully collected weather data for {latitude}, {longitude}")
            return weather_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather data: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in get_current_weather: {str(e)}")
            return None
    
    def get_weather_forecast(self, latitude: float, longitude: float, days: int = 5) -> List[Dict]:
        """Get weather forecast for a specific location"""
        try:
            params = {
                'lat': latitude,
                'lon': longitude,
                'appid': self.api_key,
                'units': 'metric',
                'cnt': min(days * 8, 40)  # 8 forecasts per day, max 40
            }
            
            response = self.session.get(f"{self.base_url}/forecast", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            forecast_data = []
            
            for item in data.get('list', []):
                weather_item = self._parse_weather_data(item)
                if weather_item:
                    forecast_data.append(weather_item)
            
            logger.info(f"Successfully collected {len(forecast_data)} forecast points")
            return forecast_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch weather forecast: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_weather_forecast: {str(e)}")
            return []
    
    def get_historical_weather(self, latitude: float, longitude: float, 
                            start_date: datetime, end_date: datetime) -> List[Dict]:
        """Get historical weather data for a specific location and time range"""
        try:
            # OpenWeatherMap One Call API for historical data
            params = {
                'lat': latitude,
                'lon': longitude,
                'appid': self.api_key,
                'units': 'metric',
                'dt': int(start_date.timestamp())
            }
            
            response = self.session.get(f"{self.base_url}/onecall/timemachine", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            historical_data = []
            
            for item in data.get('data', []):
                weather_item = self._parse_weather_data(item)
                if weather_item:
                    historical_data.append(weather_item)
            
            logger.info(f"Successfully collected {len(historical_data)} historical weather points")
            return historical_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch historical weather: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Unexpected error in get_historical_weather: {str(e)}")
            return []
    
    def _parse_weather_data(self, raw_data: Dict) -> Optional[Dict]:
        """Parse weather data from API response"""
        try:
            # Handle different API response formats
            if 'main' in raw_data:
                # Current weather format
                main = raw_data['main']
                weather = raw_data['weather'][0] if raw_data.get('weather') else {}
                wind = raw_data.get('wind', {})
                clouds = raw_data.get('clouds', {})
                rain = raw_data.get('rain', {})
                snow = raw_data.get('snow', {})
                
                timestamp = datetime.fromtimestamp(raw_data.get('dt', 0))
            else:
                # Forecast or historical format
                main = raw_data
                weather = raw_data.get('weather', [{}])[0]
                wind = raw_data.get('wind', {})
                clouds = raw_data.get('clouds', {})
                rain = raw_data.get('rain', {})
                snow = raw_data.get('snow', {})
                
                timestamp = datetime.fromtimestamp(raw_data.get('dt', 0))
            
            weather_data = {
                'timestamp': timestamp,
                'temperature_celsius': main.get('temp', 0),
                'humidity_percent': main.get('humidity', 0),
                'pressure_hpa': main.get('pressure', 0),
                'wind_speed_ms': wind.get('speed', 0),
                'wind_direction_degrees': wind.get('deg', 0),
                'precipitation_mm': (rain.get('1h', 0) or rain.get('3h', 0) or 
                                   snow.get('1h', 0) or snow.get('3h', 0)),
                'weather_condition': weather.get('main', ''),
                'weather_description': weather.get('description', ''),
                'visibility_km': raw_data.get('visibility', 0) / 1000 if raw_data.get('visibility') else None,
                'uv_index': raw_data.get('uvi', 0),
                'cloudiness_percent': clouds.get('all', 0)
            }
            
            return weather_data
            
        except Exception as e:
            logger.error(f"Error parsing weather data: {str(e)}")
            return None
    
    def collect_weather_for_stations(self, stations: List[Dict]) -> List[Dict]:
        """Collect weather data for multiple charging stations"""
        weather_data = []
        
        for station in stations:
            try:
                lat = station.get('latitude')
                lon = station.get('longitude')
                
                if lat and lon:
                    current_weather = self.get_current_weather(lat, lon)
                    if current_weather:
                        current_weather['station_id'] = station.get('id')
                        weather_data.append(current_weather)
                    
                    # Add delay to respect free tier rate limits (1 call per second)
                    time.sleep(1.0)
                    
            except Exception as e:
                logger.error(f"Error collecting weather for station {station.get('id')}: {str(e)}")
                continue
        
        logger.info(f"Collected weather data for {len(weather_data)} stations")
        return weather_data
