"""
Business Intelligence Module
Generates actionable insights for charging network optimization
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from database.connection import db_connection

logger = logging.getLogger(__name__)

class BusinessIntelligence:
    def __init__(self):
        self.db = db_connection
    
    def generate_network_insights(self) -> Dict:
        """
        Generate high-level insights for the entire charging network
        """
        try:
            supabase = self.db.get_supabase()
            
            # Get all stations
            stations = supabase.table('charging_stations').select('*').execute().data
            usage_data = supabase.table('usage_data').select('*').execute().data
            charging_points = supabase.table('charging_points').select('*').execute().data
            
            if not stations:
                return {'error': 'No station data available'}
            
            stations_df = pd.DataFrame(stations)
            usage_df = pd.DataFrame(usage_data) if usage_data else pd.DataFrame()
            points_df = pd.DataFrame(charging_points) if charging_points else pd.DataFrame()
            
            insights = {
                'network_overview': self._generate_network_overview(stations_df, usage_df, points_df),
                'performance_metrics': self._calculate_performance_metrics(stations_df, usage_df),
                'optimization_recommendations': self._generate_optimization_recommendations(stations_df, usage_df, points_df),
                'revenue_insights': self._analyze_revenue(usage_df),
                'capacity_analysis': self._analyze_capacity(stations_df, points_df, usage_df)
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating network insights: {str(e)}")
            return {'error': str(e)}
    
    def _generate_network_overview(self, stations_df: pd.DataFrame, 
                                  usage_df: pd.DataFrame, 
                                  points_df: pd.DataFrame) -> Dict:
        """Generate network overview statistics"""
        total_stations = len(stations_df)
        total_points = len(points_df) if not points_df.empty else 0
        total_sessions = len(usage_df) if not usage_df.empty else 0
        
        # Geographic distribution
        states = stations_df['state'].value_counts().to_dict() if 'state' in stations_df.columns else {}
        cities = stations_df['city'].value_counts().to_dict() if 'city' in stations_df.columns else {}
        
        # Operators
        operators = stations_df['operator'].value_counts().to_dict() if 'operator' in stations_df.columns else {}
        
        return {
            'total_stations': int(total_stations),
            'total_charging_points': int(total_points),
            'total_sessions': int(total_sessions),
            'stations_by_state': states,
            'stations_by_city': cities,
            'stations_by_operator': operators,
            'avg_points_per_station': float(total_points / total_stations) if total_stations > 0 else 0
        }
    
    def _calculate_performance_metrics(self, stations_df: pd.DataFrame, 
                                      usage_df: pd.DataFrame) -> Dict:
        """Calculate performance metrics"""
        if usage_df.empty:
            return {'error': 'No usage data available'}
        
        # Session metrics
        total_sessions = len(usage_df)
        total_energy = usage_df['energy_consumed_kwh'].sum() if 'energy_consumed_kwh' in usage_df.columns else 0
        avg_energy_per_session = total_energy / total_sessions if total_sessions > 0 else 0
        
        # Duration metrics
        avg_duration = usage_df['duration_minutes'].mean() if 'duration_minutes' in usage_df.columns else 0
        
        # Cost metrics
        total_revenue = usage_df['cost'].sum() if 'cost' in usage_df.columns else 0
        avg_cost_per_session = total_revenue / total_sessions if total_sessions > 0 else 0
        
        # Utilization
        active_stations = usage_df['station_id'].nunique() if 'station_id' in usage_df.columns else 0
        total_stations = len(stations_df)
        utilization_rate = (active_stations / total_stations * 100) if total_stations > 0 else 0
        
        return {
            'total_sessions': int(total_sessions),
            'total_energy_kwh': float(total_energy),
            'avg_energy_per_session_kwh': float(avg_energy_per_session),
            'avg_duration_minutes': float(avg_duration),
            'total_revenue': float(total_revenue),
            'avg_cost_per_session': float(avg_cost_per_session),
            'active_stations': int(active_stations),
            'utilization_rate_percent': float(utilization_rate)
        }
    
    def _generate_optimization_recommendations(self, stations_df: pd.DataFrame,
                                              usage_df: pd.DataFrame,
                                              points_df: pd.DataFrame) -> List[Dict]:
        """Generate optimization recommendations"""
        recommendations = []
        
        if usage_df.empty:
            return [{'type': 'data_collection', 'priority': 'high', 
                    'recommendation': 'Collect more usage data to generate insights'}]
        
        # 1. Underutilized stations
        station_usage = usage_df.groupby('station_id').size()
        low_usage_stations = station_usage[station_usage < station_usage.quantile(0.25)]
        
        if len(low_usage_stations) > 0:
            recommendations.append({
                'type': 'underutilized_stations',
                'priority': 'medium',
                'recommendation': f'{len(low_usage_stations)} stations have low usage. Consider marketing or relocation.',
                'affected_stations': low_usage_stations.index.tolist()[:10]  # Limit to 10
            })
        
        # 2. High demand stations (need more capacity)
        high_usage_stations = station_usage[station_usage > station_usage.quantile(0.75)]
        
        if len(high_usage_stations) > 0:
            # Check if they have enough charging points
            if not points_df.empty:
                points_per_station = points_df.groupby('station_id').size()
                for station_id in high_usage_stations.index[:5]:  # Check top 5
                    station_points = points_per_station.get(station_id, 0)
                    if station_points < 4:  # Threshold
                        recommendations.append({
                            'type': 'capacity_expansion',
                            'priority': 'high',
                            'recommendation': f'Station {station_id} has high demand but only {station_points} charging points. Consider expansion.',
                            'station_id': station_id
                        })
        
        # 3. Revenue optimization
        if 'cost' in usage_df.columns:
            revenue_by_station = usage_df.groupby('station_id')['cost'].sum()
            low_revenue_stations = revenue_by_station[revenue_by_station < revenue_by_station.quantile(0.25)]
            
            if len(low_revenue_stations) > 0:
                recommendations.append({
                    'type': 'pricing_optimization',
                    'priority': 'medium',
                    'recommendation': f'{len(low_revenue_stations)} stations have low revenue. Review pricing strategy.',
                    'affected_stations': low_revenue_stations.index.tolist()[:10]
                })
        
        # 4. Geographic gaps
        if 'state' in stations_df.columns and 'city' in stations_df.columns:
            state_coverage = stations_df.groupby('state').size()
            low_coverage_states = state_coverage[state_coverage < state_coverage.quantile(0.25)]
            
            if len(low_coverage_states) > 0:
                recommendations.append({
                    'type': 'geographic_expansion',
                    'priority': 'low',
                    'recommendation': f'Consider expanding to states with low coverage: {low_coverage_states.index.tolist()}',
                    'states': low_coverage_states.index.tolist()
                })
        
        return recommendations
    
    def _analyze_revenue(self, usage_df: pd.DataFrame) -> Dict:
        """Analyze revenue patterns"""
        if usage_df.empty or 'cost' not in usage_df.columns:
            return {'error': 'No revenue data available'}
        
        total_revenue = usage_df['cost'].sum()
        avg_revenue_per_session = usage_df['cost'].mean()
        
        # Revenue by station
        revenue_by_station = usage_df.groupby('station_id')['cost'].sum().sort_values(ascending=False)
        
        # Revenue trends (if timestamp available)
        revenue_trends = {}
        if 'session_start' in usage_df.columns:
            usage_df['session_start'] = pd.to_datetime(usage_df['session_start'])
            usage_df['date'] = usage_df['session_start'].dt.date
            daily_revenue = usage_df.groupby('date')['cost'].sum()
            revenue_trends = {
                'daily_revenue': daily_revenue.to_dict(),
                'trend': 'increasing' if len(daily_revenue) > 1 and daily_revenue.iloc[-1] > daily_revenue.iloc[0] else 'stable'
            }
        
        return {
            'total_revenue': float(total_revenue),
            'avg_revenue_per_session': float(avg_revenue_per_session),
            'top_revenue_stations': revenue_by_station.head(10).to_dict(),
            'revenue_trends': revenue_trends
        }
    
    def _analyze_capacity(self, stations_df: pd.DataFrame,
                         points_df: pd.DataFrame,
                         usage_df: pd.DataFrame) -> Dict:
        """Analyze charging capacity and utilization"""
        if points_df.empty:
            return {'error': 'No charging point data available'}
        
        # Points per station
        points_per_station = points_df.groupby('station_id').size()
        avg_points = points_per_station.mean()
        
        # Power capacity
        if 'power_kw' in points_df.columns:
            total_power = points_df['power_kw'].sum()
            avg_power_per_point = points_df['power_kw'].mean()
        else:
            total_power = 0
            avg_power_per_point = 0
        
        # Utilization (if usage data available)
        utilization = {}
        if not usage_df.empty and 'station_id' in usage_df.columns:
            station_usage = usage_df.groupby('station_id').size()
            for station_id in points_per_station.index:
                sessions = station_usage.get(station_id, 0)
                points = points_per_station.get(station_id, 1)
                utilization[station_id] = {
                    'sessions': int(sessions),
                    'points': int(points),
                    'utilization_ratio': float(sessions / points) if points > 0 else 0
                }
        
        return {
            'total_charging_points': int(len(points_df)),
            'avg_points_per_station': float(avg_points),
            'total_power_capacity_kw': float(total_power),
            'avg_power_per_point_kw': float(avg_power_per_point),
            'station_utilization': utilization
        }
    
    def generate_station_insights(self, station_id: str) -> Dict:
        """Generate insights for a specific station"""
        try:
            supabase = self.db.get_supabase()
            
            # Get station data
            station = supabase.table('charging_stations').select('*').eq('id', station_id).execute().data
            if not station:
                return {'error': 'Station not found'}
            
            station = station[0]
            
            # Get related data
            usage_data = supabase.table('usage_data').select('*').eq('station_id', station_id).execute().data
            points_data = supabase.table('charging_points').select('*').eq('station_id', station_id).execute().data
            weather_data = supabase.table('weather_data').select('*').eq('station_id', station_id).execute().data
            traffic_data = supabase.table('traffic_data').select('*').eq('station_id', station_id).execute().data
            
            usage_df = pd.DataFrame(usage_data) if usage_data else pd.DataFrame()
            points_df = pd.DataFrame(points_data) if points_data else pd.DataFrame()
            
            insights = {
                'station_info': station,
                'usage_statistics': self._calculate_station_usage_stats(usage_df),
                'capacity_info': self._calculate_station_capacity(points_df),
                'recommendations': self._generate_station_recommendations(station, usage_df, points_df)
            }
            
            return insights
            
        except Exception as e:
            logger.error(f"Error generating station insights: {str(e)}")
            return {'error': str(e)}
    
    def _calculate_station_usage_stats(self, usage_df: pd.DataFrame) -> Dict:
        """Calculate usage statistics for a station"""
        if usage_df.empty:
            return {'error': 'No usage data'}
        
        return {
            'total_sessions': int(len(usage_df)),
            'total_energy_kwh': float(usage_df['energy_consumed_kwh'].sum()) if 'energy_consumed_kwh' in usage_df.columns else 0,
            'avg_duration_minutes': float(usage_df['duration_minutes'].mean()) if 'duration_minutes' in usage_df.columns else 0,
            'total_revenue': float(usage_df['cost'].sum()) if 'cost' in usage_df.columns else 0
        }
    
    def _calculate_station_capacity(self, points_df: pd.DataFrame) -> Dict:
        """Calculate capacity information for a station"""
        if points_df.empty:
            return {'error': 'No charging point data'}
        
        return {
            'total_points': int(len(points_df)),
            'total_power_kw': float(points_df['power_kw'].sum()) if 'power_kw' in points_df.columns else 0,
            'avg_power_kw': float(points_df['power_kw'].mean()) if 'power_kw' in points_df.columns else 0
        }
    
    def _generate_station_recommendations(self, station: Dict,
                                         usage_df: pd.DataFrame,
                                         points_df: pd.DataFrame) -> List[Dict]:
        """Generate recommendations for a specific station"""
        recommendations = []
        
        if usage_df.empty:
            recommendations.append({
                'type': 'data_collection',
                'priority': 'high',
                'recommendation': 'Collect more usage data to generate insights'
            })
            return recommendations
        
        # Check capacity vs demand
        if not points_df.empty:
            sessions = len(usage_df)
            points = len(points_df)
            utilization = sessions / points if points > 0 else 0
            
            if utilization > 50:  # High utilization
                recommendations.append({
                    'type': 'capacity',
                    'priority': 'high',
                    'recommendation': f'High utilization ({utilization:.1f} sessions/point). Consider adding more charging points.'
                })
        
        # Check revenue
        if 'cost' in usage_df.columns:
            total_revenue = usage_df['cost'].sum()
            avg_revenue = usage_df['cost'].mean()
            
            if avg_revenue < 10:  # Low average revenue
                recommendations.append({
                    'type': 'pricing',
                    'priority': 'medium',
                    'recommendation': f'Low average revenue per session (${avg_revenue:.2f}). Review pricing strategy.'
                })
        
        return recommendations

