"""
Correlation Analysis Module
Analyzes correlations between weather, traffic, and EV charging usage patterns
"""

import logging
import pandas as pd
import numpy as np
from scipy import stats
from typing import List, Dict, Optional, Tuple
from database.connection import db_connection

logger = logging.getLogger(__name__)

class CorrelationAnalysis:
    def __init__(self):
        self.db = db_connection
    
    def analyze_weather_usage_correlation(self, station_id: Optional[str] = None) -> Dict:
        """
        Analyze correlation between weather conditions and charging usage
        
        Returns correlation coefficients and insights
        """
        try:
            supabase = self.db.get_supabase()
            
            # Get weather data
            weather_query = supabase.table('weather_data').select('*')
            if station_id:
                weather_query = weather_query.eq('station_id', station_id)
            weather_data = weather_query.execute().data
            
            # Get usage data
            usage_query = supabase.table('usage_data').select('*')
            if station_id:
                usage_query = usage_query.eq('station_id', station_id)
            usage_data = usage_query.execute().data
            
            if not weather_data or not usage_data:
                logger.warning("Insufficient data for weather-usage correlation")
                return {'error': 'Insufficient data'}
            
            weather_df = pd.DataFrame(weather_data)
            usage_df = pd.DataFrame(usage_data)
            
            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
            usage_df['session_start'] = pd.to_datetime(usage_df['session_start'])
            
            # Merge data by hour
            weather_df['hour'] = weather_df['timestamp'].dt.floor('H')
            usage_df['hour'] = usage_df['session_start'].dt.floor('H')
            
            hourly_usage = usage_df.groupby(['station_id', 'hour']).agg({
                'id': 'count',
                'energy_consumed_kwh': 'sum' if 'energy_consumed_kwh' in usage_df.columns else lambda x: 0
            }).reset_index()
            hourly_usage.columns = ['station_id', 'hour', 'session_count', 'total_energy']
            
            # Merge weather and usage
            merged = weather_df.merge(
                hourly_usage,
                on=['station_id', 'hour'],
                how='inner'
            )
            
            if merged.empty:
                return {'error': 'No overlapping data points'}
            
            correlations = {}
            insights = []
            
            # Temperature correlation
            if 'temperature_celsius' in merged.columns:
                temp_corr = merged['temperature_celsius'].corr(merged['session_count'])
                correlations['temperature_vs_sessions'] = float(temp_corr) if not np.isnan(temp_corr) else 0
                
                if abs(temp_corr) > 0.3:
                    direction = 'positive' if temp_corr > 0 else 'negative'
                    insights.append(f"Temperature shows {direction} correlation ({temp_corr:.2f}) with session count")
            
            # Precipitation correlation
            if 'precipitation_mm' in merged.columns:
                precip_corr = merged['precipitation_mm'].corr(merged['session_count'])
                correlations['precipitation_vs_sessions'] = float(precip_corr) if not np.isnan(precip_corr) else 0
                
                if abs(precip_corr) > 0.3:
                    direction = 'positive' if precip_corr > 0 else 'negative'
                    insights.append(f"Precipitation shows {direction} correlation ({precip_corr:.2f}) with session count")
            
            # Weather condition analysis
            if 'weather_condition' in merged.columns:
                condition_usage = merged.groupby('weather_condition')['session_count'].mean()
                correlations['weather_condition_impact'] = condition_usage.to_dict()
                insights.append(f"Weather conditions with highest usage: {condition_usage.nlargest(3).to_dict()}")
            
            return {
                'correlations': correlations,
                'insights': insights,
                'data_points': len(merged),
                'station_id': station_id
            }
            
        except Exception as e:
            logger.error(f"Error analyzing weather-usage correlation: {str(e)}")
            return {'error': str(e)}
    
    def analyze_traffic_usage_correlation(self, station_id: Optional[str] = None) -> Dict:
        """
        Analyze correlation between traffic conditions and charging usage
        """
        try:
            supabase = self.db.get_supabase()
            
            # Get traffic data
            traffic_query = supabase.table('traffic_data').select('*')
            if station_id:
                traffic_query = traffic_query.eq('station_id', station_id)
            traffic_data = traffic_query.execute().data
            
            # Get usage data
            usage_query = supabase.table('usage_data').select('*')
            if station_id:
                usage_query = usage_query.eq('station_id', station_id)
            usage_data = usage_query.execute().data
            
            if not traffic_data or not usage_data:
                logger.warning("Insufficient data for traffic-usage correlation")
                return {'error': 'Insufficient data'}
            
            traffic_df = pd.DataFrame(traffic_data)
            usage_df = pd.DataFrame(usage_data)
            
            traffic_df['timestamp'] = pd.to_datetime(traffic_df['timestamp'])
            usage_df['session_start'] = pd.to_datetime(usage_df['session_start'])
            
            # Merge data by hour
            traffic_df['hour'] = traffic_df['timestamp'].dt.floor('H')
            usage_df['hour'] = usage_df['session_start'].dt.floor('H')
            
            hourly_usage = usage_df.groupby(['station_id', 'hour']).agg({
                'id': 'count',
                'energy_consumed_kwh': 'sum' if 'energy_consumed_kwh' in usage_df.columns else lambda x: 0
            }).reset_index()
            hourly_usage.columns = ['station_id', 'hour', 'session_count', 'total_energy']
            
            # Merge traffic and usage
            merged = traffic_df.merge(
                hourly_usage,
                on=['station_id', 'hour'],
                how='inner'
            )
            
            if merged.empty:
                return {'error': 'No overlapping data points'}
            
            correlations = {}
            insights = []
            
            # Traffic density correlation
            if 'traffic_density' in merged.columns:
                density_corr = merged['traffic_density'].corr(merged['session_count'])
                correlations['traffic_density_vs_sessions'] = float(density_corr) if not np.isnan(density_corr) else 0
                
                if abs(density_corr) > 0.3:
                    direction = 'positive' if density_corr > 0 else 'negative'
                    insights.append(f"Traffic density shows {direction} correlation ({density_corr:.2f}) with session count")
            
            # Congestion level analysis
            if 'congestion_level' in merged.columns:
                congestion_usage = merged.groupby('congestion_level')['session_count'].mean()
                correlations['congestion_level_impact'] = congestion_usage.to_dict()
                insights.append(f"Congestion levels with highest usage: {congestion_usage.nlargest(3).to_dict()}")
            
            # Average speed correlation
            if 'average_speed_kmh' in merged.columns:
                speed_corr = merged['average_speed_kmh'].corr(merged['session_count'])
                correlations['average_speed_vs_sessions'] = float(speed_corr) if not np.isnan(speed_corr) else 0
            
            return {
                'correlations': correlations,
                'insights': insights,
                'data_points': len(merged),
                'station_id': station_id
            }
            
        except Exception as e:
            logger.error(f"Error analyzing traffic-usage correlation: {str(e)}")
            return {'error': str(e)}
    
    def analyze_combined_correlation(self, station_id: Optional[str] = None) -> Dict:
        """
        Analyze combined correlation of weather and traffic with usage
        """
        try:
            supabase = self.db.get_supabase()
            
            # Get all data
            weather_query = supabase.table('weather_data').select('*')
            traffic_query = supabase.table('traffic_data').select('*')
            usage_query = supabase.table('usage_data').select('*')
            
            if station_id:
                weather_query = weather_query.eq('station_id', station_id)
                traffic_query = traffic_query.eq('station_id', station_id)
                usage_query = usage_query.eq('station_id', station_id)
            
            weather_data = weather_query.execute().data
            traffic_data = traffic_query.execute().data
            usage_data = usage_query.execute().data
            
            if not weather_data or not traffic_data or not usage_data:
                return {'error': 'Insufficient data for combined analysis'}
            
            weather_df = pd.DataFrame(weather_data)
            traffic_df = pd.DataFrame(traffic_data)
            usage_df = pd.DataFrame(usage_data)
            
            # Prepare time-based aggregations
            weather_df['timestamp'] = pd.to_datetime(weather_df['timestamp'])
            traffic_df['timestamp'] = pd.to_datetime(traffic_df['timestamp'])
            usage_df['session_start'] = pd.to_datetime(usage_df['session_start'])
            
            weather_df['hour'] = weather_df['timestamp'].dt.floor('H')
            traffic_df['hour'] = traffic_df['timestamp'].dt.floor('H')
            usage_df['hour'] = usage_df['session_start'].dt.floor('H')
            
            # Aggregate usage by hour
            hourly_usage = usage_df.groupby(['station_id', 'hour']).agg({
                'id': 'count',
                'energy_consumed_kwh': 'sum' if 'energy_consumed_kwh' in usage_df.columns else lambda x: 0
            }).reset_index()
            hourly_usage.columns = ['station_id', 'hour', 'session_count', 'total_energy']
            
            # Merge all data
            merged = weather_df.merge(
                traffic_df,
                on=['station_id', 'hour'],
                how='inner',
                suffixes=('_weather', '_traffic')
            ).merge(
                hourly_usage,
                on=['station_id', 'hour'],
                how='inner'
            )
            
            if merged.empty:
                return {'error': 'No overlapping data points'}
            
            # Calculate multiple correlations
            correlations = {}
            
            # Weather features
            if 'temperature_celsius' in merged.columns:
                temp_corr = merged['temperature_celsius'].corr(merged['session_count'])
                correlations['temperature'] = float(temp_corr) if not np.isnan(temp_corr) else 0
            
            if 'precipitation_mm' in merged.columns:
                precip_corr = merged['precipitation_mm'].corr(merged['session_count'])
                correlations['precipitation'] = float(precip_corr) if not np.isnan(precip_corr) else 0
            
            # Traffic features
            if 'traffic_density' in merged.columns:
                density_corr = merged['traffic_density'].corr(merged['session_count'])
                correlations['traffic_density'] = float(density_corr) if not np.isnan(density_corr) else 0
            
            if 'average_speed_kmh' in merged.columns:
                speed_corr = merged['average_speed_kmh'].corr(merged['session_count'])
                correlations['average_speed'] = float(speed_corr) if not np.isnan(speed_corr) else 0
            
            # Generate insights
            insights = []
            strong_correlations = {k: v for k, v in correlations.items() if abs(v) > 0.5}
            if strong_correlations:
                insights.append(f"Strong correlations found: {strong_correlations}")
            
            return {
                'correlations': correlations,
                'insights': insights,
                'data_points': len(merged),
                'station_id': station_id
            }
            
        except Exception as e:
            logger.error(f"Error in combined correlation analysis: {str(e)}")
            return {'error': str(e)}
    
    def generate_correlation_report(self, station_id: Optional[str] = None) -> Dict:
        """
        Generate comprehensive correlation report
        """
        weather_corr = self.analyze_weather_usage_correlation(station_id)
        traffic_corr = self.analyze_traffic_usage_correlation(station_id)
        combined_corr = self.analyze_combined_correlation(station_id)
        
        return {
            'weather_correlation': weather_corr,
            'traffic_correlation': traffic_corr,
            'combined_correlation': combined_corr,
            'summary': {
                'weather_data_points': weather_corr.get('data_points', 0),
                'traffic_data_points': traffic_corr.get('data_points', 0),
                'combined_data_points': combined_corr.get('data_points', 0)
            }
        }

