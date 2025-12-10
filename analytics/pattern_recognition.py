"""
Pattern Recognition Module
Identifies usage patterns, trends, and recurring behaviors in EV charging data
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
from database.connection import db_connection

logger = logging.getLogger(__name__)

class PatternRecognition:
    def __init__(self):
        self.db = db_connection
    
    def identify_usage_patterns(self, station_id: Optional[str] = None) -> List[Dict]:
        """
        Identify usage patterns across stations or for a specific station
        
        Patterns identified:
        - Peak usage hours
        - Day-of-week patterns
        - Seasonal trends
        - Usage spikes
        - Low usage periods
        """
        try:
            supabase = self.db.get_supabase()
            
            # Get usage data
            query = supabase.table('usage_data').select('*')
            if station_id:
                query = query.eq('station_id', station_id)
            
            usage_data = query.execute().data
            
            if not usage_data:
                logger.warning("No usage data found for pattern recognition")
                return []
            
            df = pd.DataFrame(usage_data)
            df['session_start'] = pd.to_datetime(df['session_start'])
            
            patterns = []
            
            # 1. Peak Usage Hours Pattern
            peak_hours = self._identify_peak_hours(df)
            patterns.append({
                'pattern_type': 'peak_hours',
                'station_id': station_id,
                'description': f"Peak usage occurs during hours: {peak_hours}",
                'details': {'peak_hours': peak_hours},
                'confidence': 0.85
            })
            
            # 2. Day-of-Week Pattern
            day_pattern = self._identify_day_of_week_pattern(df)
            patterns.append({
                'pattern_type': 'day_of_week',
                'station_id': station_id,
                'description': f"Weekday vs Weekend pattern: {day_pattern}",
                'details': day_pattern,
                'confidence': 0.80
            })
            
            # 3. Usage Spike Pattern
            spikes = self._identify_usage_spikes(df)
            if spikes:
                patterns.append({
                    'pattern_type': 'usage_spikes',
                    'station_id': station_id,
                    'description': f"Detected {len(spikes)} usage spikes",
                    'details': {'spikes': spikes},
                    'confidence': 0.75
                })
            
            # 4. Low Usage Periods
            low_periods = self._identify_low_usage_periods(df)
            if low_periods:
                patterns.append({
                    'pattern_type': 'low_usage_periods',
                    'station_id': station_id,
                    'description': f"Identified {len(low_periods)} low usage periods",
                    'details': {'low_periods': low_periods},
                    'confidence': 0.70
                })
            
            # 5. Session Duration Pattern
            duration_pattern = self._analyze_session_duration_pattern(df)
            patterns.append({
                'pattern_type': 'session_duration',
                'station_id': station_id,
                'description': duration_pattern['description'],
                'details': duration_pattern,
                'confidence': 0.75
            })
            
            logger.info(f"Identified {len(patterns)} usage patterns")
            return patterns
            
        except Exception as e:
            logger.error(f"Error identifying usage patterns: {str(e)}")
            return []
    
    def _identify_peak_hours(self, df: pd.DataFrame) -> List[int]:
        """Identify peak usage hours"""
        if df.empty:
            return []
        
        df['hour'] = df['session_start'].dt.hour
        hourly_counts = df.groupby('hour').size()
        
        # Find hours with usage > 1.5x average
        avg_usage = hourly_counts.mean()
        peak_threshold = avg_usage * 1.5
        
        peak_hours = hourly_counts[hourly_counts >= peak_threshold].index.tolist()
        return sorted(peak_hours) if peak_hours else []
    
    def _identify_day_of_week_pattern(self, df: pd.DataFrame) -> Dict:
        """Identify weekday vs weekend patterns"""
        if df.empty:
            return {}
        
        df['is_weekend'] = df['session_start'].dt.dayofweek >= 5
        df['day_name'] = df['session_start'].dt.day_name()
        
        weekday_sessions = df[~df['is_weekend']].shape[0]
        weekend_sessions = df[df['is_weekend']].shape[0]
        
        weekday_energy = df[~df['is_weekend']]['energy_consumed_kwh'].sum() if 'energy_consumed_kwh' in df.columns else 0
        weekend_energy = df[df['is_weekend']]['energy_consumed_kwh'].sum() if 'energy_consumed_kwh' in df.columns else 0
        
        return {
            'weekday_sessions': int(weekday_sessions),
            'weekend_sessions': int(weekend_sessions),
            'weekday_energy_kwh': float(weekday_energy) if weekday_energy else 0,
            'weekend_energy_kwh': float(weekend_energy) if weekend_energy else 0,
            'pattern': 'weekend_heavy' if weekend_sessions > weekday_sessions else 'weekday_heavy'
        }
    
    def _identify_usage_spikes(self, df: pd.DataFrame) -> List[Dict]:
        """Identify unusual usage spikes"""
        if df.empty:
            return []
        
        df['date'] = df['session_start'].dt.date
        daily_counts = df.groupby('date').size()
        
        # Use z-score to identify spikes
        mean_count = daily_counts.mean()
        std_count = daily_counts.std()
        
        spikes = []
        for date, count in daily_counts.items():
            if std_count > 0:
                z_score = (count - mean_count) / std_count
                if z_score > 2:  # More than 2 standard deviations
                    spikes.append({
                        'date': str(date),
                        'session_count': int(count),
                        'z_score': float(z_score)
                    })
        
        return spikes
    
    def _identify_low_usage_periods(self, df: pd.DataFrame) -> List[Dict]:
        """Identify periods with unusually low usage"""
        if df.empty:
            return []
        
        df['date'] = df['session_start'].dt.date
        daily_counts = df.groupby('date').size()
        
        mean_count = daily_counts.mean()
        std_count = daily_counts.std()
        
        low_periods = []
        for date, count in daily_counts.items():
            if std_count > 0:
                z_score = (count - mean_count) / std_count
                if z_score < -1.5:  # More than 1.5 standard deviations below mean
                    low_periods.append({
                        'date': str(date),
                        'session_count': int(count),
                        'z_score': float(z_score)
                    })
        
        return low_periods
    
    def _analyze_session_duration_pattern(self, df: pd.DataFrame) -> Dict:
        """Analyze session duration patterns"""
        if df.empty or 'duration_minutes' not in df.columns:
            return {'description': 'No duration data available'}
        
        durations = df['duration_minutes'].dropna()
        
        if durations.empty:
            return {'description': 'No valid duration data'}
        
        return {
            'avg_duration_minutes': float(durations.mean()),
            'median_duration_minutes': float(durations.median()),
            'min_duration_minutes': float(durations.min()),
            'max_duration_minutes': float(durations.max()),
            'std_duration_minutes': float(durations.std()),
            'description': f"Average session duration: {durations.mean():.1f} minutes"
        }
    
    def identify_seasonal_trends(self, station_id: Optional[str] = None) -> List[Dict]:
        """Identify seasonal trends in usage"""
        try:
            supabase = self.db.get_supabase()
            
            query = supabase.table('usage_data').select('*')
            if station_id:
                query = query.eq('station_id', station_id)
            
            usage_data = query.execute().data
            
            if not usage_data:
                return []
            
            df = pd.DataFrame(usage_data)
            df['session_start'] = pd.to_datetime(df['session_start'])
            df['month'] = df['session_start'].dt.month
            df['season'] = df['session_start'].dt.month.map({
                12: 'Winter', 1: 'Winter', 2: 'Winter',
                3: 'Spring', 4: 'Spring', 5: 'Spring',
                6: 'Summer', 7: 'Summer', 8: 'Summer',
                9: 'Fall', 10: 'Fall', 11: 'Fall'
            })
            
            seasonal_stats = df.groupby('season').agg({
                'id': 'count',
                'energy_consumed_kwh': 'sum' if 'energy_consumed_kwh' in df.columns else lambda x: 0
            }).to_dict('index')
            
            trends = []
            for season, stats in seasonal_stats.items():
                trends.append({
                    'season': season,
                    'session_count': int(stats['id']),
                    'total_energy_kwh': float(stats.get('energy_consumed_kwh', 0))
                })
            
            return trends
            
        except Exception as e:
            logger.error(f"Error identifying seasonal trends: {str(e)}")
            return []
    
    def identify_location_patterns(self) -> List[Dict]:
        """Identify patterns based on geographic location"""
        try:
            supabase = self.db.get_supabase()
            
            # Get station data with usage statistics
            stations = supabase.table('charging_stations').select('*').execute().data
            usage_data = supabase.table('usage_data').select('*').execute().data
            
            if not stations or not usage_data:
                return []
            
            stations_df = pd.DataFrame(stations)
            usage_df = pd.DataFrame(usage_data)
            
            # Group by state/city
            if 'state' in stations_df.columns:
                state_patterns = usage_df.merge(
                    stations_df[['id', 'state', 'city']],
                    left_on='station_id',
                    right_on='id',
                    how='left'
                ).groupby('state').agg({
                    'id': 'count',
                    'energy_consumed_kwh': 'sum' if 'energy_consumed_kwh' in usage_df.columns else lambda x: 0
                }).to_dict('index')
                
                patterns = []
                for state, stats in state_patterns.items():
                    patterns.append({
                        'location_type': 'state',
                        'location': state,
                        'session_count': int(stats['id']),
                        'total_energy_kwh': float(stats.get('energy_consumed_kwh', 0))
                    })
                
                return patterns
            
            return []
            
        except Exception as e:
            logger.error(f"Error identifying location patterns: {str(e)}")
            return []

