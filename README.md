# EV Charging Station Analytics

A comprehensive data collection and analysis system for Electric Vehicle (EV) charging station usage patterns, integrating multiple data sources including charging stations, weather, and traffic data.

## Features

- **Data Collection**: Automated collection from OpenChargeMap, OpenWeatherMap, and HERE Maps APIs
- **Data Processing**: Cleaning, validation, and feature engineering
- **Anomaly Detection**: Machine learning-based detection of unusual usage patterns
- **Database Storage**: Supabase MySQL integration for scalable data storage
- **Docker Support**: Containerized deployment for easy setup and scaling
- **Scheduled Collection**: Automated data collection at configurable intervals

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │  Data Processing│    │   Database      │
│                 │    │                 │    │                 │
│ • OpenChargeMap │───▶│ • Data Cleaning │───▶│ • Supabase      │
│ • OpenWeather   │    │ • Feature Eng.  │    │ • MySQL         │
│ • HERE Maps     │    │ • Anomaly Det.  │    │ • Analytics     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Data Sources

- **OpenChargeMap API**: Comprehensive EV charging station locations and metadata
- **OpenWeatherMap API**: Weather conditions and forecasts
- **HERE Maps API**: Traffic flow and congestion data
- **Government Energy Portals**: Energy consumption and pricing data

## Database Schema

The system stores data in the following main tables:

- `charging_stations`: Station metadata and location information
- `charging_points`: Individual charging point specifications
- `usage_data`: Historical usage sessions and energy consumption
- `weather_data`: Weather conditions correlated with station locations
- `traffic_data`: Traffic flow and congestion data
- `anomaly_detection`: Detected anomalies and unusual patterns
- `engineered_features`: Processed features for machine learning

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Supabase account with MySQL database
- API keys for data sources:
  - OpenWeatherMap API key
  - HERE Maps API key

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd ev-analytics
   ```

2. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your actual values
   ```

3. **Configure Supabase database**
   - Create a new Supabase project
   - Run the database schema:
   ```bash
   # Execute database/schema.sql in your Supabase SQL editor
   ```

4. **Build and run with Docker**
   ```bash
   docker-compose up --build
   ```

### Environment Variables

Create a `.env` file with the following variables:

```env
# Supabase Configuration
SUPABASE_URL=your_supabase_url_here
SUPABASE_KEY=your_supabase_anon_key_here

# API Keys
OPENWEATHER_API_KEY=your_openweather_api_key_here
HERE_API_KEY=your_here_maps_api_key_here

# Database Configuration
DB_HOST=your_supabase_db_host
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your_db_password

# Application Configuration
DATA_COLLECTION_INTERVAL=3600  # seconds (1 hour)
LOG_LEVEL=INFO
```

## Usage

### Running the Application

**One-time data collection:**
```bash
docker-compose run --rm ev-analytics python main.py
```

**Scheduled data collection:**
```bash
# Set RUN_MODE=scheduled in .env
docker-compose up -d
```

### Data Collection Modes

1. **Initial Setup**: Collects charging stations and their metadata
2. **Weather Integration**: Fetches weather data for all station locations
3. **Traffic Analysis**: Collects traffic flow data around stations
4. **Feature Engineering**: Creates derived features for analysis
5. **Anomaly Detection**: Identifies unusual usage patterns

## Data Analysis Features

### Feature Engineering

The system creates several engineered features:

- **Temporal Features**: Hour of day, day of week, weekend/holiday indicators
- **Weather Correlations**: Temperature, precipitation, storm conditions
- **Traffic Impact**: Congestion levels, average speeds
- **Usage Patterns**: Peak hours, downtime analysis
- **Energy Metrics**: Consumption per traffic density

### Anomaly Detection

Uses machine learning to detect:

- Unusual downtime patterns
- Unexpected usage spikes
- Weather-related anomalies
- Traffic correlation anomalies
- Seasonal pattern deviations

## API Endpoints

The system provides REST API endpoints for data access:

- `GET /api/stations` - List all charging stations
- `GET /api/stations/{id}/weather` - Weather data for a station
- `GET /api/stations/{id}/traffic` - Traffic data for a station
- `GET /api/anomalies` - List detected anomalies
- `GET /api/statistics` - System statistics

## Monitoring and Logging

- **Application Logs**: Stored in `logs/ev_analytics.log`
- **Data Collection Logs**: Tracked in database
- **Error Handling**: Comprehensive error logging and recovery
- **Performance Metrics**: Collection timing and success rates

## Development

### Project Structure

```
ev-analytics/
├── data_collectors/          # Data source integrations
│   ├── openchargemap_collector.py
│   ├── weather_collector.py
│   └── traffic_collector.py
├── data_processing/          # Data cleaning and analysis
│   └── data_processor.py
├── data_storage/            # Database operations
│   └── database_manager.py
├── database/                # Database configuration
│   ├── connection.py
│   └── schema.sql
├── main.py                  # Main application
├── config.py               # Configuration management
├── requirements.txt        # Python dependencies
├── Dockerfile             # Container configuration
└── docker-compose.yml     # Orchestration
```

### Adding New Data Sources

1. Create a new collector in `data_collectors/`
2. Implement the collection interface
3. Add data cleaning logic in `data_processor.py`
4. Update database schema if needed
5. Integrate into main collection cycle

### Extending Analysis

1. Add new features in `data_processor.py`
2. Implement new anomaly detection algorithms
3. Create additional database tables
4. Update the main processing pipeline

## Troubleshooting

### Common Issues

1. **API Rate Limits**: The system includes delays between requests
2. **Database Connection**: Check Supabase credentials and network access
3. **Missing Data**: Verify API keys and data source availability
4. **Memory Usage**: Monitor container resource usage

### Logs

Check application logs for detailed error information:
```bash
docker-compose logs ev-analytics
```

