#!/usr/bin/env python3
"""
EV Charging Station Analytics Application
Main application for collecting, processing, and storing EV charging data
"""

import logging
import schedule
import time
import os
from datetime import datetime
from typing import List, Dict

# Import data collectors
from data_collectors.openchargemap_collector import OpenChargeMapCollector
from data_collectors.weather_collector import WeatherCollector
from data_collectors.traffic_collector import TrafficCollector

# Import data processing
from data_processing.data_processor import DataProcessor

# Import database management
from data_storage.database_manager import DatabaseManager

# Import configuration
from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/ev_analytics.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class EVAnalyticsApp:
    def __init__(self):
        self.ocm_collector = OpenChargeMapCollector()
        self.weather_collector = WeatherCollector()
        self.traffic_collector = TrafficCollector()
        self.data_processor = DataProcessor()
        self.db_manager = DatabaseManager()
        
        
        # Create necessary directories
        os.makedirs("logs", exist_ok=True)
    
    def collect_charging_stations(self, country_code: str = 'US', max_results: int = None) -> List[Dict]:
        """Collect charging station data from OpenChargeMap"""
        if max_results is None:
            max_results = Config.MAX_STATIONS_PER_COLLECTION
        
        logger.info(f"Starting charging station collection for {country_code} (max: {max_results})")
        
        try:
            stations = self.ocm_collector.get_charging_stations(
                country_code=country_code,
                max_results=max_results
            )
            
            if stations:
                # Clean the data
                cleaned_stations = self.data_processor.clean_charging_station_data(stations)
                
                # Store in database
                success = self.db_manager.insert_charging_stations(cleaned_stations)
                
                if success:
                    logger.info(f"Successfully collected and stored {len(cleaned_stations)} charging stations")
                    
                    # Store charging points for each station
                    all_charging_points = []
                    for station in cleaned_stations:
                        if 'charging_points' in station and station['charging_points']:
                            for point in station['charging_points']:
                                point['station_id'] = station['id']
                                all_charging_points.append(point)
                    
                    if all_charging_points:
                        points_success = self.db_manager.insert_charging_points(all_charging_points)
                        if points_success:
                            logger.info(f"Successfully stored {len(all_charging_points)} charging points")
                        else:
                            logger.warning("Failed to store some charging points")
                    
                    self.db_manager.log_data_collection(
                        data_source="OpenChargeMap",
                        collection_type="charging_stations",
                        records_collected=len(cleaned_stations),
                        status="success"
                    )
                else:
                    logger.error("Failed to store charging stations in database")
                    self.db_manager.log_data_collection(
                        data_source="OpenChargeMap",
                        collection_type="charging_stations",
                        records_collected=len(cleaned_stations),
                        status="failed",
                        error_message="Database insertion failed"
                    )
                
                return cleaned_stations
            else:
                logger.warning("No charging stations collected")
                return []
                
        except Exception as e:
            logger.error(f"Error collecting charging stations: {str(e)}")
            self.db_manager.log_data_collection(
                data_source="OpenChargeMap",
                collection_type="charging_stations",
                records_collected=0,
                status="failed",
                error_message=str(e)
            )
            return []
    
    def collect_weather_data(self, stations: List[Dict]) -> List[Dict]:
        """Collect weather data for charging stations"""
        if not Config.WEATHER_COLLECTION_ENABLED:
            logger.info("Weather collection disabled to save API calls")
            return []
        
        # Limit stations to stay within free tier (max 1000 calls/day)
        max_weather_stations = min(len(stations), 50)  # Conservative limit
        limited_stations = stations[:max_weather_stations]
        
        logger.info(f"Starting weather data collection for {len(limited_stations)} stations (limited for free tier)")
        
        try:
            weather_data = self.weather_collector.collect_weather_for_stations(limited_stations)
            
            if weather_data:
                # Clean the data
                cleaned_weather = self.data_processor.clean_weather_data(weather_data)
                
                # Store in database
                success = self.db_manager.insert_weather_data(cleaned_weather)
                
                if success:
                    logger.info(f"Successfully collected and stored {len(cleaned_weather)} weather records")
                    self.db_manager.log_data_collection(
                        data_source="OpenWeatherMap",
                        collection_type="weather_data",
                        records_collected=len(cleaned_weather),
                        status="success"
                    )
                else:
                    logger.error("Failed to store weather data in database")
                
                return cleaned_weather
            else:
                logger.warning("No weather data collected")
                return []
                
        except Exception as e:
            logger.error(f"Error collecting weather data: {str(e)}")
            return []
    
    def collect_traffic_data(self, stations: List[Dict]) -> List[Dict]:
        """Collect traffic data for charging stations"""
        if not Config.TRAFFIC_COLLECTION_ENABLED:
            logger.info("Traffic collection disabled to save API calls")
            return []
        
        # Limit stations to stay within free tier (max 1000 transactions/month)
        max_traffic_stations = min(len(stations), Config.MAX_TRAFFIC_STATIONS)
        limited_stations = stations[:max_traffic_stations]
        
        logger.info(f"Starting traffic data collection for {len(limited_stations)} stations (limited for free tier)")
        
        try:
            traffic_data = self.traffic_collector.collect_traffic_for_stations(limited_stations)
            
            if traffic_data:
                # Clean the data
                cleaned_traffic = self.data_processor.clean_traffic_data(traffic_data)
                
                # Store in database
                success = self.db_manager.insert_traffic_data(cleaned_traffic)
                
                if success:
                    logger.info(f"Successfully collected and stored {len(cleaned_traffic)} traffic records")
                    self.db_manager.log_data_collection(
                        data_source="HERE_Maps",
                        collection_type="traffic_data",
                        records_collected=len(cleaned_traffic),
                        status="success"
                    )
                else:
                    logger.error("Failed to store traffic data in database")
                
                return cleaned_traffic
            else:
                logger.warning("No traffic data collected")
                return []
                
        except Exception as e:
            logger.error(f"Error collecting traffic data: {str(e)}")
            return []
    
    
    def process_and_analyze(self, stations: List[Dict], weather_data: List[Dict], 
                           traffic_data: List[Dict]) -> None:
        """Process data and perform analysis"""
        logger.info("Starting data processing and analysis")
        
        try:
            # Engineer features
            features = self.data_processor.engineer_features(stations, weather_data, traffic_data)
            
            if features:
                # Store engineered features
                success = self.db_manager.insert_engineered_features(features)
                if success:
                    logger.info(f"Successfully stored {len(features)} engineered features")
                
                # Detect anomalies
                anomalies = self.data_processor.detect_anomalies(features)
                
                if anomalies:
                    # Store anomalies
                    success = self.db_manager.insert_anomalies(anomalies)
                    if success:
                        logger.info(f"Successfully detected and stored {len(anomalies)} anomalies")
                    else:
                        logger.error("Failed to store anomalies in database")
                else:
                    logger.info("No anomalies detected")
            else:
                logger.warning("No features engineered")
                
        except Exception as e:
            logger.error(f"Error in data processing and analysis: {str(e)}")
    
    def run_data_collection_cycle(self) -> None:
        """Run a complete data collection cycle"""
        logger.info("Starting data collection cycle")
        start_time = datetime.now()
        
        try:
            # Step 1: Collect charging stations
            stations = self.collect_charging_stations()
            
            if not stations:
                logger.warning("No stations collected, skipping other data collection")
                return
            
            # Step 2: Collect current weather data
            weather_data = self.collect_weather_data(stations)
            
            # Step 3: Collect current traffic data (disabled for free tier)
            traffic_data = self.collect_traffic_data(stations)
            
            # Step 4: Historical data collection disabled for free tier
            logger.info("Historical data collection disabled to save API calls")
            
            # Step 5: Process and analyze data
            self.process_and_analyze(stations, weather_data, traffic_data)
            
            # Log completion
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"Data collection cycle completed in {duration:.2f} seconds")
            
        except Exception as e:
            logger.error(f"Error in data collection cycle: {str(e)}")
    
    
    def run_scheduled_collection(self) -> None:
        """Run scheduled data collection"""
        logger.info("Setting up scheduled data collection")
        
        # Schedule data collection every 2 hours (free tier friendly)
        schedule.every(2).hours.do(self.run_data_collection_cycle)
        
        # Run initial collection
        self.run_data_collection_cycle()
        
        # Keep running
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    
    def run_once(self) -> None:
        """Run data collection once"""
        logger.info("Running one-time data collection")
        self.run_data_collection_cycle()
    
    def get_statistics(self) -> None:
        """Get and display statistics"""
        logger.info("Retrieving statistics")
        
        try:
            stats = self.db_manager.get_station_statistics()
            
            if stats:
                logger.info(f"Retrieved statistics for {len(stats)} stations")
                for stat in stats[:5]:  # Show first 5
                    logger.info(f"Station {stat['station_id']}: {stat['name']} - "
                              f"Weather: {stat['weather_records']}, "
                              f"Traffic: {stat['traffic_records']}, "
                              f"Anomalies: {stat['anomaly_count']}")
            else:
                logger.info("No statistics available")
                
        except Exception as e:
            logger.error(f"Error retrieving statistics: {str(e)}")

def main():
    """Main application entry point"""
    app = EVAnalyticsApp()
    
    # Check if running in scheduled mode or once
    if os.getenv('RUN_MODE', 'once').lower() == 'scheduled':
        logger.info("Starting in scheduled mode")
        app.run_scheduled_collection()
    else:
        logger.info("Running in one-time mode")
        app.run_once()
        
        # Show statistics
        app.get_statistics()

if __name__ == "__main__":
    main()
