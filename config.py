import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Supabase Configuration
    SUPABASE_URL = 'https://ecnskdjrsyknbwneqxmi.supabase.co'
    SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImVjbnNrZGpyc3lrbmJ3bmVxeG1pIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjA0NTkwOTEsImV4cCI6MjA3NjAzNTA5MX0.hDwb_7stz5fFzzZxWnrSSlqLz_TSHEGyWRBFYHZlDdw'
    
    # API Keys
    OPENCHARGEMAP_API_KEY = os.getenv('OPENCHARGEMAP_API_KEY')
    OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
    HERE_API_KEY = os.getenv('HERE_API_KEY')
    
    # Application Configuration
    DATA_COLLECTION_INTERVAL = int(os.getenv('DATA_COLLECTION_INTERVAL', 7200))  # 2 hours
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    # Free Tier Optimizations
    MAX_STATIONS_PER_COLLECTION = int(os.getenv('MAX_STATIONS_PER_COLLECTION', 100))
    WEATHER_COLLECTION_ENABLED = os.getenv('WEATHER_COLLECTION_ENABLED', 'true').lower() == 'true'
    TRAFFIC_COLLECTION_ENABLED = os.getenv('TRAFFIC_COLLECTION_ENABLED', 'true').lower() == 'true'
    MAX_TRAFFIC_STATIONS = int(os.getenv('MAX_TRAFFIC_STATIONS', 5))  # Very limited for free tier
    
    # Data Sources
    OPENCHARGEMAP_API_URL = "https://api.openchargemap.io/v3/poi"
    OPENWEATHER_API_URL = "https://api.openweathermap.org/data/2.5"
    HERE_TRAFFIC_API_URL = "https://traffic.ls.hereapi.com/traffic/6.3/flow.json"
