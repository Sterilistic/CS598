import logging
import random
from datetime import datetime, timedelta
from typing import List, Dict
from data_collectors.weather_collector import WeatherCollector
from data_collectors.traffic_collector import TrafficCollector
from data_collectors.openchargemap_collector import OpenChargeMapCollector

logger = logging.getLogger(__name__)

class HistoricalDataCollector:
    def __init__(self):
        self.weather_collector = None
        self.traffic_collector = None
        self.ocm_collector = None
    
    def set_collectors(self, weather_collector: WeatherCollector, 
                      traffic_collector: TrafficCollector, 
                      ocm_collector: OpenChargeMapCollector):
        """Set references to other collectors"""
        self.weather_collector = weather_collector
        self.traffic_collector = traffic_collector
        self.ocm_collector = ocm_collector
    
    def collect_historical_weather(self, stations: List[Dict], days_back: int = 7) -> List[Dict]:
        """Collect historical weather data (placeholder - requires paid API)"""
        logger.info(f"Historical weather collection not implemented (requires paid API)")
        return []
    
    def collect_historical_usage_patterns(self, stations: List[Dict], days_back: int = 7) -> List[Dict]:
        """Generate synthetic historical usage patterns"""
        logger.info(f"Generating synthetic usage patterns for {len(stations)} stations")
        
        usage_data = []
        for station in stations:
            # Generate synthetic usage data for the past week
            for day in range(days_back):
                date = datetime.now() - timedelta(days=day)
                
                # Generate 1-5 sessions per day
                num_sessions = random.randint(1, 5)
                for session in range(num_sessions):
                    session_data = {
                        'station_id': station['id'],
                        'point_id': f"{station['id']}_point_{session}",
                        'session_start': (date + timedelta(hours=random.randint(6, 22))).isoformat(),
                        'session_end': (date + timedelta(hours=random.randint(6, 22), minutes=random.randint(30, 180))).isoformat(),
                        'energy_consumed_kwh': round(random.uniform(10, 80), 2),
                        'duration_minutes': random.randint(30, 180),
                        'cost': round(random.uniform(5, 50), 2),
                        'user_type': random.choice(['individual', 'fleet', 'commercial']),
                        'created_at': datetime.now().isoformat()
                    }
                    usage_data.append(session_data)
        
        logger.info(f"Generated {len(usage_data)} synthetic usage records")
        return usage_data
    
    def collect_historical_energy_consumption(self, stations: List[Dict], days_back: int = 7) -> List[Dict]:
        """Generate synthetic historical energy consumption data"""
        logger.info(f"Generating synthetic energy consumption for {len(stations)} stations")
        
        energy_data = []
        for station in stations:
            for day in range(days_back):
                date = datetime.now() - timedelta(days=day)
                
                # Generate daily energy consumption
                total_energy = round(random.uniform(50, 500), 2)
                peak_energy = round(total_energy * random.uniform(0.6, 0.8), 2)
                off_peak_energy = total_energy - peak_energy
                
                energy_record = {
                    'station_id': station['id'],
                    'date': date.date().isoformat(),
                    'total_energy_kwh': total_energy,
                    'peak_hour_energy': peak_energy,
                    'off_peak_energy': off_peak_energy,
                    'session_count': random.randint(5, 25),
                    'avg_session_duration': random.randint(45, 120),
                    'created_at': datetime.now().isoformat()
                }
                energy_data.append(energy_record)
        
        logger.info(f"Generated {len(energy_data)} synthetic energy consumption records")
        return energy_data
