import logging
from datetime import datetime
from typing import List, Dict, Optional
from database.connection import db_connection

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db = db_connection
    
    def insert_charging_stations(self, stations: List[Dict]) -> bool:
        """Insert charging stations into database"""
        try:
            supabase = self.db.get_supabase()
            
            # Prepare data for Supabase
            for station in stations:
                station_data = {
                    'id': station.get('id'),
                    'name': station.get('name'),
                    'latitude': station.get('latitude'),
                    'longitude': station.get('longitude'),
                    'address': station.get('address'),
                    'city': station.get('city'),
                    'state': station.get('state'),
                    'country': station.get('country'),
                    'operator': station.get('operator'),
                    'network': station.get('network'),
                    'status': station.get('status'),
                    'access_type': station.get('access_type'),
                    'pricing_info': station.get('pricing_info'),
                    'amenities': station.get('amenities'),
                    'created_at': station.get('created_at', datetime.now()).isoformat() if station.get('created_at') else datetime.now().isoformat(),
                    'updated_at': station.get('updated_at', datetime.now()).isoformat() if station.get('updated_at') else datetime.now().isoformat()
                }
                
                # Insert or update using upsert
                supabase.table('charging_stations').upsert(station_data).execute()
            
            logger.info(f"Successfully inserted {len(stations)} charging stations")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting charging stations: {str(e)}")
            return False
    
    def insert_charging_points(self, points: List[Dict]) -> bool:
        """Insert charging points into database"""
        try:
            supabase = self.db.get_supabase()
            
            # Prepare data for Supabase
            for point in points:
                point_record = {
                    'id': point.get('id'),
                    'station_id': point.get('station_id'),
                    'connector_type': point.get('connector_type'),
                    'power_kw': float(point.get('power_kw', 0)) if point.get('power_kw') is not None else None,
                    'voltage': float(point.get('voltage', 0)) if point.get('voltage') is not None else None,
                    'amperage': float(point.get('amperage', 0)) if point.get('amperage') is not None else None,
                    'status': point.get('status'),
                    'last_updated': point.get('last_updated', datetime.now()).isoformat() if point.get('last_updated') else datetime.now().isoformat(),
                    'created_at': datetime.now().isoformat()
                }
                
                supabase.table('charging_points').upsert(point_record).execute()
            
            logger.info(f"Successfully inserted {len(points)} charging points")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting charging points: {str(e)}")
            return False
    
    def insert_weather_data(self, weather_data: List[Dict]) -> bool:
        """Insert weather data into database"""
        try:
            supabase = self.db.get_supabase()
            
            # Prepare data for Supabase
            for weather in weather_data:
                weather_record = {
                    'station_id': weather.get('station_id'),
                    'timestamp': weather.get('timestamp', datetime.now()).isoformat() if weather.get('timestamp') else datetime.now().isoformat(),
                    'temperature_celsius': float(weather.get('temperature_celsius', 0)) if weather.get('temperature_celsius') is not None else None,
                    'humidity_percent': int(weather.get('humidity_percent', 0)) if weather.get('humidity_percent') is not None else None,
                    'pressure_hpa': float(weather.get('pressure_hpa', 0)) if weather.get('pressure_hpa') is not None else None,
                    'wind_speed_ms': float(weather.get('wind_speed_ms', 0)) if weather.get('wind_speed_ms') is not None else None,
                    'wind_direction_degrees': int(weather.get('wind_direction_degrees', 0)) if weather.get('wind_direction_degrees') is not None else None,
                    'precipitation_mm': float(weather.get('precipitation_mm', 0)) if weather.get('precipitation_mm') is not None else None,
                    'weather_condition': weather.get('weather_condition'),
                    'visibility_km': float(weather.get('visibility_km', 0)) if weather.get('visibility_km') is not None else None,
                    'uv_index': int(weather.get('uv_index', 0)) if weather.get('uv_index') is not None else None,
                    'created_at': datetime.now().isoformat()
                }
                
                supabase.table('weather_data').upsert(weather_record).execute()
            
            logger.info(f"Successfully inserted {len(weather_data)} weather records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting weather data: {str(e)}")
            return False
    
    def insert_traffic_data(self, traffic_data: List[Dict]) -> bool:
        """Insert traffic data into database"""
        try:
            session = self.db.get_session()
            
            for traffic in traffic_data:
                query = text("""
                    INSERT INTO traffic_data 
                    (station_id, timestamp, traffic_density, average_speed_kmh, 
                     congestion_level, road_type, distance_to_station_km, created_at)
                    VALUES 
                    (:station_id, :timestamp, :traffic_density, :average_speed_kmh,
                     :congestion_level, :road_type, :distance_to_station_km, :created_at)
                """)
                
                session.execute(query, traffic)
            
            session.commit()
            logger.info(f"Successfully inserted {len(traffic_data)} traffic records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting traffic data: {str(e)}")
            session.rollback()
            return False
    
    def insert_engineered_features(self, features: List[Dict]) -> bool:
        """Insert engineered features into database"""
        try:
            supabase = self.db.get_supabase()
            
            # Prepare data for Supabase
            for feature in features:
                feature_record = {
                    'station_id': feature.get('station_id'),
                    'date': feature.get('date', datetime.now().date()).isoformat() if feature.get('date') else datetime.now().date().isoformat(),
                    'hour_of_day': int(feature.get('hour_of_day', 0)) if feature.get('hour_of_day') is not None else None,
                    'day_of_week': int(feature.get('day_of_week', 0)) if feature.get('day_of_week') is not None else None,
                    'is_weekend': bool(feature.get('is_weekend', False)) if feature.get('is_weekend') is not None else False,
                    'is_holiday': bool(feature.get('is_holiday', False)) if feature.get('is_holiday') is not None else False,
                    'avg_downtime_minutes': float(feature.get('avg_downtime_minutes', 0)) if feature.get('avg_downtime_minutes') is not None else None,
                    'energy_consumption_per_traffic': float(feature.get('energy_consumption_per_traffic', 0)) if feature.get('energy_consumption_per_traffic') is not None else None,
                    'usage_spike_during_storm': bool(feature.get('usage_spike_during_storm', False)) if feature.get('usage_spike_during_storm') is not None else False,
                    'peak_usage_hours': int(feature.get('peak_usage_hours', 0)) if feature.get('peak_usage_hours') is not None else None,
                    'avg_wait_time_minutes': float(feature.get('avg_wait_time_minutes', 0)) if feature.get('avg_wait_time_minutes') is not None else None,
                    'total_sessions': int(feature.get('total_sessions', 0)) if feature.get('total_sessions') is not None else None,
                    'total_energy_kwh': float(feature.get('total_energy_kwh', 0)) if feature.get('total_energy_kwh') is not None else None,
                    'created_at': datetime.now().isoformat()
                }
                
                supabase.table('engineered_features').upsert(feature_record).execute()
            
            logger.info(f"Successfully inserted {len(features)} feature records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting engineered features: {str(e)}")
            return False
    
    def insert_anomalies(self, anomalies: List[Dict]) -> bool:
        """Insert anomaly detection results into database"""
        try:
            supabase = self.db.get_supabase()
            
            # Prepare data for Supabase
            for anomaly in anomalies:
                anomaly_record = {
                    'station_id': anomaly.get('station_id'),
                    'anomaly_type': anomaly.get('anomaly_type'),
                    'severity_score': float(anomaly.get('severity_score', 0)) if anomaly.get('severity_score') is not None else None,
                    'detected_at': anomaly.get('detected_at', datetime.now()).isoformat() if anomaly.get('detected_at') else datetime.now().isoformat(),
                    'description': anomaly.get('description'),
                    'is_resolved': bool(anomaly.get('is_resolved', False)) if anomaly.get('is_resolved') is not None else False,
                    'created_at': datetime.now().isoformat()
                }
                
                supabase.table('anomaly_detection').upsert(anomaly_record).execute()
            
            logger.info(f"Successfully inserted {len(anomalies)} anomaly records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting anomalies: {str(e)}")
            return False
    
    def log_data_collection(self, data_source: str, collection_type: str, 
                          records_collected: int, status: str, error_message: str = None) -> bool:
        """Log data collection activities"""
        try:
            supabase = self.db.get_supabase()
            
            log_entry = {
                'data_source': data_source,
                'collection_type': collection_type,
                'records_collected': records_collected,
                'status': status,
                'error_message': error_message,
                'started_at': datetime.now().isoformat(),
                'completed_at': datetime.now().isoformat(),
                'created_at': datetime.now().isoformat()
            }
            
            supabase.table('data_collection_log').insert(log_entry).execute()
            
            logger.info(f"Logged data collection: {data_source} - {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error logging data collection: {str(e)}")
            return False
    
    def insert_usage_data(self, usage_data: List[Dict]) -> bool:
        """Insert usage data into database"""
        try:
            session = self.db.get_session()
            
            for usage in usage_data:
                query = text("""
                    INSERT INTO usage_data 
                    (station_id, session_start, session_end, energy_consumed_kwh, 
                     duration_minutes, cost, user_type, created_at)
                    VALUES 
                    (:station_id, :session_start, :session_end, :energy_consumed_kwh,
                     :duration_minutes, :cost, :user_type, :created_at)
                """)
                
                session.execute(query, usage)
            
            session.commit()
            logger.info(f"Successfully inserted {len(usage_data)} usage records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting usage data: {str(e)}")
            session.rollback()
            return False
    
    def insert_energy_consumption(self, energy_data: List[Dict]) -> bool:
        """Insert energy consumption data into database"""
        try:
            session = self.db.get_session()
            
            for energy in energy_data:
                query = text("""
                    INSERT INTO energy_consumption 
                    (station_id, date, total_energy_kwh, peak_hour_energy, 
                     off_peak_energy, session_count, avg_session_duration, created_at)
                    VALUES 
                    (:station_id, :date, :total_energy_kwh, :peak_hour_energy,
                     :off_peak_energy, :session_count, :avg_session_duration, :created_at)
                """)
                
                session.execute(query, energy)
            
            session.commit()
            logger.info(f"Successfully inserted {len(energy_data)} energy consumption records")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting energy consumption data: {str(e)}")
            session.rollback()
            return False
    
    def get_station_statistics(self, station_id: str = None) -> List[Dict]:
        """Get statistics for charging stations"""
        try:
            query = """
                SELECT 
                    s.id,
                    s.name,
                    s.city,
                    s.state,
                    COUNT(DISTINCT w.id) as weather_records,
                    COUNT(DISTINCT t.id) as traffic_records,
                    COUNT(DISTINCT a.id) as anomaly_count,
                    AVG(w.temperature_celsius) as avg_temperature,
                    AVG(t.traffic_density) as avg_traffic_density
                FROM charging_stations s
                LEFT JOIN weather_data w ON s.id = w.station_id
                LEFT JOIN traffic_data t ON s.id = t.station_id
                LEFT JOIN anomaly_detection a ON s.id = a.station_id
            """
            
            if station_id:
                query += " WHERE s.id = :station_id"
                params = {'station_id': station_id}
            else:
                params = {}
            
            query += " GROUP BY s.id, s.name, s.city, s.state"
            
            result = self.db.execute_query(query, params)
            
            statistics = []
            for row in result:
                stats = {
                    'station_id': row[0],
                    'name': row[1],
                    'city': row[2],
                    'state': row[3],
                    'weather_records': row[4] or 0,
                    'traffic_records': row[5] or 0,
                    'anomaly_count': row[6] or 0,
                    'avg_temperature': float(row[7]) if row[7] else None,
                    'avg_traffic_density': float(row[8]) if row[8] else None
                }
                statistics.append(stats)
            
            return statistics
            
        except Exception as e:
            logger.error(f"Error getting station statistics: {str(e)}")
            return []
