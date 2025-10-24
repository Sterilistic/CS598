import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN

logger = logging.getLogger(__name__)

class DataProcessor:
    def __init__(self):
        self.scaler = StandardScaler()
        self.anomaly_detector = IsolationForest(contamination=0.1, random_state=42)
    
    def clean_charging_station_data(self, stations: List[Dict]) -> List[Dict]:
        """Clean and validate charging station data"""
        cleaned_stations = []
        
        for station in stations:
            try:
                # Validate required fields
                if not station.get('id') or not station.get('latitude') or not station.get('longitude'):
                    continue
                
                # Clean and validate coordinates
                lat = float(station.get('latitude', 0))
                lon = float(station.get('longitude', 0))
                
                if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
                    continue
                
                # Clean text fields
                cleaned_station = {
                    'id': str(station.get('id', '')).strip(),
                    'name': str(station.get('name', '')).strip()[:500],
                    'latitude': lat,
                    'longitude': lon,
                    'address': str(station.get('address', '')).strip()[:1000],
                    'city': str(station.get('city', '')).strip()[:255],
                    'state': str(station.get('state', '')).strip()[:255],
                    'country': str(station.get('country', '')).strip()[:255],
                    'operator': str(station.get('operator', '')).strip()[:255],
                    'network': str(station.get('network', '')).strip()[:255],
                    'status': str(station.get('status', 'Unknown')).strip()[:50],
                    'access_type': str(station.get('access_type', '')).strip()[:100],
                    'pricing_info': str(station.get('pricing_info', '')).strip()[:1000],
                    'amenities': str(station.get('amenities', '')).strip()[:1000],
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                }
                
                cleaned_stations.append(cleaned_station)
                
            except Exception as e:
                logger.error(f"Error cleaning station data: {str(e)}")
                continue
        
        logger.info(f"Cleaned {len(cleaned_stations)} out of {len(stations)} stations")
        return cleaned_stations
    
    def clean_weather_data(self, weather_data: List[Dict]) -> List[Dict]:
        """Clean and validate weather data"""
        cleaned_weather = []
        
        for weather in weather_data:
            try:
                # Validate required fields
                if not weather.get('timestamp') or not weather.get('station_id'):
                    continue
                
                # Clean numeric fields
                cleaned_weather_item = {
                    'station_id': str(weather.get('station_id', '')).strip(),
                    'timestamp': weather.get('timestamp'),
                    'temperature_celsius': self._clean_numeric(weather.get('temperature_celsius'), -50, 60),
                    'humidity_percent': self._clean_numeric(weather.get('humidity_percent'), 0, 100),
                    'pressure_hpa': self._clean_numeric(weather.get('pressure_hpa'), 800, 1100),
                    'wind_speed_ms': self._clean_numeric(weather.get('wind_speed_ms'), 0, 100),
                    'wind_direction_degrees': self._clean_numeric(weather.get('wind_direction_degrees'), 0, 360),
                    'precipitation_mm': self._clean_numeric(weather.get('precipitation_mm'), 0, 1000),
                    'weather_condition': str(weather.get('weather_condition', '')).strip()[:100],
                    'visibility_km': self._clean_numeric(weather.get('visibility_km'), 0, 50),
                    'uv_index': self._clean_numeric(weather.get('uv_index'), 0, 15),
                    'created_at': datetime.now()
                }
                
                cleaned_weather.append(cleaned_weather_item)
                
            except Exception as e:
                logger.error(f"Error cleaning weather data: {str(e)}")
                continue
        
        logger.info(f"Cleaned {len(cleaned_weather)} out of {len(weather_data)} weather records")
        return cleaned_weather
    
    def clean_traffic_data(self, traffic_data: List[Dict]) -> List[Dict]:
        """Clean and validate traffic data"""
        cleaned_traffic = []
        
        for traffic in traffic_data:
            try:
                # Validate required fields
                if not traffic.get('timestamp') or not traffic.get('station_id'):
                    continue
                
                # Clean numeric fields
                cleaned_traffic_item = {
                    'station_id': str(traffic.get('station_id', '')).strip(),
                    'timestamp': traffic.get('timestamp'),
                    'traffic_density': self._clean_numeric(traffic.get('traffic_density'), 0, 1000),
                    'average_speed_kmh': self._clean_numeric(traffic.get('average_speed_kmh'), 0, 200),
                    'congestion_level': str(traffic.get('congestion_level', 'unknown')).strip()[:50],
                    'road_type': str(traffic.get('road_type', 'highway')).strip()[:100],
                    'distance_to_station_km': self._clean_numeric(traffic.get('distance_to_station_km'), 0, 100),
                    'created_at': datetime.now()
                }
                
                cleaned_traffic.append(cleaned_traffic_item)
                
            except Exception as e:
                logger.error(f"Error cleaning traffic data: {str(e)}")
                continue
        
        logger.info(f"Cleaned {len(cleaned_traffic)} out of {len(traffic_data)} traffic records")
        return cleaned_traffic
    
    def _clean_numeric(self, value, min_val: float, max_val: float) -> Optional[float]:
        """Clean and validate numeric values"""
        try:
            if value is None:
                return None
            
            num_val = float(value)
            if min_val <= num_val <= max_val:
                return num_val
            else:
                return None
        except (ValueError, TypeError):
            return None
    
    def engineer_features(self, stations: List[Dict], weather_data: List[Dict], 
                         traffic_data: List[Dict]) -> List[Dict]:
        """Engineer features for machine learning and analysis"""
        features = []
        
        try:
            # Convert to DataFrames for easier processing
            stations_df = pd.DataFrame(stations)
            weather_df = pd.DataFrame(weather_data)
            traffic_df = pd.DataFrame(traffic_data)
            
            for station in stations:
                station_id = station.get('id')
                if not station_id:
                    continue
                
                # Filter data for this station
                station_weather = weather_df[weather_df['station_id'] == station_id] if not weather_df.empty else pd.DataFrame()
                station_traffic = traffic_df[traffic_df['station_id'] == station_id] if not traffic_df.empty else pd.DataFrame()
                
                # Calculate features
                feature_row = {
                    'station_id': station_id,
                    'date': datetime.now().date(),
                    'hour_of_day': datetime.now().hour,
                    'day_of_week': datetime.now().weekday(),
                    'is_weekend': datetime.now().weekday() >= 5,
                    'is_holiday': self._is_holiday(datetime.now()),
                    'avg_downtime_minutes': self._calculate_avg_downtime(station_weather),
                    'energy_consumption_per_traffic': self._calculate_energy_traffic_ratio(station_weather, station_traffic),
                    'usage_spike_during_storm': self._detect_storm_usage_spike(station_weather),
                    'peak_usage_hours': self._identify_peak_hours(station_weather),
                    'avg_wait_time_minutes': self._calculate_avg_wait_time(station_traffic),
                    'total_sessions': len(station_weather),
                    'total_energy_kwh': self._calculate_total_energy(station_weather),
                    'created_at': datetime.now()
                }
                
                features.append(feature_row)
            
            logger.info(f"Engineered features for {len(features)} stations")
            return features
            
        except Exception as e:
            logger.error(f"Error engineering features: {str(e)}")
            return []
    
    def _is_holiday(self, date: datetime) -> bool:
        """Check if date is a holiday (simplified implementation)"""
        # This is a simplified implementation
        # In production, you would use a proper holiday calendar
        holidays = [
            (1, 1),   # New Year's Day
            (7, 4),   # Independence Day
            (12, 25), # Christmas
        ]
        return (date.month, date.day) in holidays
    
    def _calculate_avg_downtime(self, weather_df: pd.DataFrame) -> float:
        """Calculate average downtime based on weather conditions"""
        if weather_df.empty:
            return 0.0
        
        # Simplified calculation - in reality, you'd need actual downtime data
        storm_conditions = weather_df['weather_condition'].str.contains('storm|rain|snow', case=False, na=False)
        return storm_conditions.sum() * 30.0  # Assume 30 minutes downtime per storm
    
    def _calculate_energy_traffic_ratio(self, weather_df: pd.DataFrame, traffic_df: pd.DataFrame) -> float:
        """Calculate energy consumption per traffic density"""
        if weather_df.empty or traffic_df.empty:
            return 0.0
        
        avg_energy = weather_df['temperature_celsius'].mean() if not weather_df.empty else 0
        avg_traffic = traffic_df['traffic_density'].mean() if not traffic_df.empty else 1
        
        return avg_energy / max(avg_traffic, 1)
    
    def _detect_storm_usage_spike(self, weather_df: pd.DataFrame) -> bool:
        """Detect if there's a usage spike during storm conditions"""
        if weather_df.empty:
            return False
        
        storm_conditions = weather_df['weather_condition'].str.contains('storm|thunder', case=False, na=False)
        return storm_conditions.any()
    
    def _identify_peak_hours(self, weather_df: pd.DataFrame) -> int:
        """Identify peak usage hours"""
        if weather_df.empty:
            return 0
        
        # Simplified - in reality, you'd analyze actual usage patterns
        return len(weather_df)
    
    def _calculate_avg_wait_time(self, traffic_df: pd.DataFrame) -> float:
        """Calculate average wait time based on traffic conditions"""
        if traffic_df.empty:
            return 0.0
        
        # Simplified calculation based on congestion
        congestion_levels = traffic_df['congestion_level'].value_counts()
        if 'severe' in congestion_levels:
            return 45.0
        elif 'moderate' in congestion_levels:
            return 20.0
        else:
            return 5.0
    
    def _calculate_total_energy(self, weather_df: pd.DataFrame) -> float:
        """Calculate total energy consumption"""
        if weather_df.empty:
            return 0.0
        
        # Simplified calculation based on temperature (higher temp = more AC usage)
        return weather_df['temperature_celsius'].sum() * 0.1
    
    def detect_anomalies(self, features: List[Dict]) -> List[Dict]:
        """Detect anomalies in the data using machine learning"""
        anomalies = []
        
        try:
            if not features:
                return anomalies
            
            # Convert to DataFrame
            df = pd.DataFrame(features)
            
            # Select numeric features for anomaly detection
            numeric_features = ['avg_downtime_minutes', 'energy_consumption_per_traffic', 
                              'avg_wait_time_minutes', 'total_sessions', 'total_energy_kwh']
            
            # Filter available features
            available_features = [col for col in numeric_features if col in df.columns]
            
            if not available_features:
                return anomalies
            
            # Prepare data for anomaly detection
            X = df[available_features].fillna(0)
            
            # Fit anomaly detector
            self.anomaly_detector.fit(X)
            anomaly_scores = self.anomaly_detector.decision_function(X)
            predictions = self.anomaly_detector.predict(X)
            
            # Identify anomalies
            for i, (idx, row) in enumerate(df.iterrows()):
                if predictions[i] == -1:  # Anomaly detected
                    anomaly = {
                        'station_id': row['station_id'],
                        'anomaly_type': 'usage_pattern',
                        'severity_score': float(abs(anomaly_scores[i])),
                        'detected_at': datetime.now(),
                        'description': f"Unusual usage pattern detected with score: {anomaly_scores[i]:.3f}",
                        'is_resolved': False,
                        'created_at': datetime.now()
                    }
                    anomalies.append(anomaly)
            
            logger.info(f"Detected {len(anomalies)} anomalies")
            return anomalies
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            return []
