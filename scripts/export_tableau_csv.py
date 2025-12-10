#!/usr/bin/env python3
"""
Script to export Tableau views to CSV files
This script queries each view from Supabase and exports to CSV for Tableau
"""

import logging
import csv
import os
from pathlib import Path
from datetime import datetime
from typing import List, Dict
from database.connection import db_connection

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# List of all Tableau views to export
TABLEAU_VIEWS = [
    'tableau_station_overview',
    'tableau_usage_patterns',
    'tableau_weather_correlation',
    'tableau_traffic_correlation',
    'tableau_energy_trends',
    'tableau_anomaly_summary',
    'tableau_station_performance',
    'tableau_hourly_usage_heatmap',
    'tableau_geographic_distribution',
    'tableau_data_collection_status'
]

def export_view_to_csv(view_name: str, output_dir: Path) -> bool:
    """
    Export a Supabase view to CSV file
    
    Note: Since Supabase Python client doesn't support querying views directly,
    we'll query the underlying tables and recreate the view logic
    """
    try:
        supabase = db_connection.get_supabase()
        
        logger.info(f"Exporting view: {view_name}")
        
        # Try to query the view directly (may not work with Supabase client)
        try:
            # Attempt to query view as a table
            result = supabase.table(view_name).select('*').execute()
            data = result.data
            
            if not data:
                logger.warning(f"No data found for view: {view_name}")
                return False
            
            # Write to CSV with proper NULL handling
            output_file = output_dir / f"{view_name}.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as f:
                if data:
                    # Clean data: replace None with empty string, handle special characters
                    cleaned_data = []
                    for row in data:
                        cleaned_row = {}
                        for key, value in row.items():
                            if value is None:
                                cleaned_row[key] = ''
                            elif isinstance(value, (dict, list)):
                                cleaned_row[key] = str(value)
                            else:
                                cleaned_row[key] = value
                        cleaned_data.append(cleaned_row)
                    
                    writer = csv.DictWriter(f, fieldnames=cleaned_data[0].keys(), quoting=csv.QUOTE_MINIMAL)
                    writer.writeheader()
                    writer.writerows(cleaned_data)
            
            logger.info(f"✓ Exported {len(data)} rows to {output_file}")
            return True
            
        except Exception as e:
            logger.warning(f"Could not query view {view_name} directly: {str(e)}")
            # If it's station_overview and it fails, create it from tables
            if view_name == 'tableau_station_overview':
                logger.info("Creating station_overview from tables instead (view query timed out)...")
                try:
                    result = create_station_overview_from_tables(output_dir)
                    if result:
                        logger.info("✓ Successfully created station_overview from tables")
                    return result
                except Exception as e2:
                    logger.error(f"Failed to create station_overview from tables: {str(e2)}")
                    return False
            return False
            
    except Exception as e:
        logger.error(f"Error exporting view {view_name}: {str(e)}")
        return False

def export_table_to_csv(table_name: str, output_dir: Path) -> bool:
    """Export a table directly to CSV"""
    try:
        supabase = db_connection.get_supabase()
        
        logger.info(f"Exporting table: {table_name}")
        
        # Query all data from table
        result = supabase.table(table_name).select('*').execute()
        data = result.data
        
        if not data:
            logger.warning(f"No data found for table: {table_name}")
            return False
        
        # Write to CSV with proper NULL handling
        output_file = output_dir / f"{table_name}.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            if data:
                # Clean data: replace None with empty string
                cleaned_data = []
                for row in data:
                    cleaned_row = {}
                    for key, value in row.items():
                        if value is None:
                            cleaned_row[key] = ''
                        elif isinstance(value, (dict, list)):
                            cleaned_row[key] = str(value)
                        else:
                            cleaned_row[key] = value
                    cleaned_data.append(cleaned_row)
                
                writer = csv.DictWriter(f, fieldnames=cleaned_data[0].keys(), quoting=csv.QUOTE_MINIMAL)
                writer.writeheader()
                writer.writerows(cleaned_data)
        
        logger.info(f"✓ Exported {len(data)} rows to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting table {table_name}: {str(e)}")
        return False

def create_station_overview_from_tables(output_dir: Path):
    """
    Create station overview by querying tables directly
    This recreates the view logic since the view query is too complex
    """
    try:
        supabase = db_connection.get_supabase()
        
        logger.info("Creating station overview from tables...")
        
        # Get all data
        stations = supabase.table('charging_stations').select('*').execute().data
        points = supabase.table('charging_points').select('*').execute().data
        usage = supabase.table('usage_data').select('*').execute().data
        weather = supabase.table('weather_data').select('station_id').execute().data
        traffic = supabase.table('traffic_data').select('station_id').execute().data
        anomalies = supabase.table('anomaly_detection').select('station_id,is_resolved').eq('is_resolved', False).execute().data
        
        if stations:
            # Create station overview data
            import pandas as pd
            
            stations_df = pd.DataFrame(stations)
            points_df = pd.DataFrame(points) if points else pd.DataFrame()
            usage_df = pd.DataFrame(usage) if usage else pd.DataFrame()
            
            # Aggregate data
            if not points_df.empty:
                points_summary = points_df.groupby('station_id').agg({
                    'id': 'count',
                    'power_kw': ['mean', 'max']
                }).reset_index()
                points_summary.columns = ['station_id', 'total_points', 'avg_power', 'max_power']
            else:
                points_summary = pd.DataFrame(columns=['station_id', 'total_points', 'avg_power', 'max_power'])
            
            if not usage_df.empty:
                usage_summary = usage_df.groupby('station_id').agg({
                    'id': 'count',
                    'energy_consumed_kwh': 'sum',
                    'duration_minutes': 'mean',
                    'cost': 'mean'
                }).reset_index()
                usage_summary.columns = ['station_id', 'total_sessions', 'total_energy', 'avg_duration', 'avg_cost']
            else:
                usage_summary = pd.DataFrame(columns=['station_id', 'total_sessions', 'total_energy', 'avg_duration', 'avg_cost'])
            
            # Get weather, traffic, and anomaly counts
            weather_df = pd.DataFrame(weather) if weather else pd.DataFrame()
            traffic_df = pd.DataFrame(traffic) if traffic else pd.DataFrame()
            anomalies_df = pd.DataFrame(anomalies) if anomalies else pd.DataFrame()
            
            weather_counts = weather_df.groupby('station_id').size().reset_index(name='weather_records_count') if not weather_df.empty else pd.DataFrame(columns=['station_id', 'weather_records_count'])
            traffic_counts = traffic_df.groupby('station_id').size().reset_index(name='traffic_records_count') if not traffic_df.empty else pd.DataFrame(columns=['station_id', 'traffic_records_count'])
            anomaly_counts = anomalies_df.groupby('station_id').size().reset_index(name='anomaly_count') if not anomalies_df.empty else pd.DataFrame(columns=['station_id', 'anomaly_count'])
            
            # Get last session date
            if not usage_df.empty:
                last_sessions = usage_df.groupby('station_id')['session_start'].max().reset_index(name='last_session_date')
            else:
                last_sessions = pd.DataFrame(columns=['station_id', 'last_session_date'])
            
            # Merge all data (drop duplicate station_id columns after each merge)
            station_overview = stations_df.merge(
                points_summary,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_points')
            )
            if 'station_id_points' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_points'])
            
            station_overview = station_overview.merge(
                usage_summary,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_usage')
            )
            if 'station_id_usage' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_usage'])
            
            station_overview = station_overview.merge(
                weather_counts,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_weather')
            )
            if 'station_id_weather' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_weather'])
            
            station_overview = station_overview.merge(
                traffic_counts,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_traffic')
            )
            if 'station_id_traffic' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_traffic'])
            
            station_overview = station_overview.merge(
                anomaly_counts,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_anomaly')
            )
            if 'station_id_anomaly' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_anomaly'])
            
            station_overview = station_overview.merge(
                last_sessions,
                left_on='id',
                right_on='station_id',
                how='left',
                suffixes=('', '_sessions')
            )
            if 'station_id_sessions' in station_overview.columns:
                station_overview = station_overview.drop(columns=['station_id_sessions'])
            
            # Drop duplicate station_id columns (keep the one from stations_df)
            if 'station_id' in station_overview.columns and 'id' in station_overview.columns:
                # Keep 'id' and rename it, drop other station_id columns
                cols_to_drop = [col for col in station_overview.columns if col == 'station_id' and col != 'id']
                if cols_to_drop:
                    station_overview = station_overview.drop(columns=cols_to_drop)
            
            # Rename columns to match view structure
            station_overview = station_overview.rename(columns={
                'id': 'station_id',
                'name': 'station_name',
                'total_points': 'total_charging_points',
                'avg_power': 'avg_power_kw',
                'max_power': 'max_power_kw',
                'total_energy': 'total_energy_kwh',
                'avg_duration': 'avg_session_duration_minutes',
                'avg_cost': 'avg_session_cost'
            })
            
            # Remove any remaining duplicate station_id columns
            if 'station_id' in station_overview.columns:
                # Keep first occurrence, drop others
                cols = list(station_overview.columns)
                seen = set()
                cols_to_keep = []
                for col in cols:
                    if col == 'station_id':
                        if 'station_id' not in seen:
                            cols_to_keep.append(col)
                            seen.add('station_id')
                    else:
                        cols_to_keep.append(col)
                station_overview = station_overview[cols_to_keep]
            
            # Fill NaN values
            station_overview = station_overview.fillna({
                'total_charging_points': 0,
                'avg_power_kw': 0,
                'max_power_kw': 0,
                'total_sessions': 0,
                'total_energy_kwh': 0,
                'avg_session_duration_minutes': 0,
                'avg_session_cost': 0,
                'weather_records_count': 0,
                'traffic_records_count': 0,
                'anomaly_count': 0
            })
            
            # Export
            output_file = output_dir / "tableau_station_overview.csv"
            station_overview.to_csv(output_file, index=False)
            logger.info(f"✓ Created station overview: {len(station_overview)} rows")
            return True
        
        # 2. Usage Patterns
        if usage:
            logger.info("Creating usage patterns...")
            usage_df = pd.DataFrame(usage)
            usage_df['session_start'] = pd.to_datetime(usage_df['session_start'])
            usage_df['date'] = usage_df['session_start'].dt.date
            usage_df['hour'] = usage_df['session_start'].dt.hour
            usage_df['day_of_week'] = usage_df['session_start'].dt.dayofweek
            usage_df['is_weekend'] = usage_df['day_of_week'] >= 5
            
            usage_patterns = usage_df.groupby(['station_id', 'date', 'hour', 'day_of_week', 'is_weekend']).agg({
                'id': 'count',
                'energy_consumed_kwh': ['sum', 'mean'],
                'duration_minutes': 'mean',
                'cost': ['sum', 'mean']
            }).reset_index()
            
            usage_patterns.columns = ['station_id', 'session_date', 'hour_of_day', 'day_of_week', 
                                     'is_weekend', 'session_count', 'total_energy', 'avg_energy',
                                     'avg_duration', 'total_cost', 'avg_cost']
            
            # Merge with station names
            if stations:
                stations_df = pd.DataFrame(stations)
                usage_patterns = usage_patterns.merge(
                    stations_df[['id', 'name', 'city', 'state']],
                    left_on='station_id',
                    right_on='id',
                    how='left'
                )
            
            output_file = output_dir / "tableau_usage_patterns.csv"
            usage_patterns.to_csv(output_file, index=False)
            logger.info(f"✓ Created usage patterns: {len(usage_patterns)} rows")
        
        # Export raw tables as well
        logger.info("\nExporting raw tables...")
        tables = ['charging_stations', 'charging_points', 'usage_data', 
                 'weather_data', 'traffic_data', 'energy_consumption',
                 'anomaly_detection', 'engineered_features', 'data_collection_log']
        
        for table in tables:
            export_table_to_csv(table, output_dir)
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating aggregated views: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function"""
    logger.info("Starting CSV export for Tableau...")
    
    # Create output directory
    output_dir = Path(__file__).parent.parent / 'tableau_exports'
    output_dir.mkdir(exist_ok=True)
    
    # Create timestamped subdirectory
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    export_dir = output_dir / timestamp
    export_dir.mkdir(exist_ok=True)
    
    logger.info(f"Export directory: {export_dir}")
    
    # First, try to export views directly
    logger.info("\nAttempting to export views directly...")
    views_exported = 0
    for view_name in TABLEAU_VIEWS:
        if export_view_to_csv(view_name, export_dir):
            views_exported += 1
    
    # If views don't exist or can't be queried, create aggregated views from tables
    if views_exported == 0:
        logger.info("\nViews not available, creating aggregated views from tables...")
        create_aggregated_views(export_dir)
    
    logger.info("\n" + "="*60)
    logger.info("Export complete!")
    logger.info(f"CSV files saved to: {export_dir}")
    logger.info("="*60)
    logger.info("\nYou can now import these CSV files into Tableau:")
    for csv_file in sorted(export_dir.glob('*.csv')):
        logger.info(f"  - {csv_file.name}")

if __name__ == "__main__":
    main()

