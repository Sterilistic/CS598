#!/usr/bin/env python3
"""
Free Tier API Usage Monitor
Tracks API usage to stay within free tier limits
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, List

class FreeTierMonitor:
    def __init__(self):
        self.usage_file = "data/api_usage.json"
        self.load_usage_data()
    
    def load_usage_data(self):
        """Load existing usage data"""
        if os.path.exists(self.usage_file):
            with open(self.usage_file, 'r') as f:
                self.usage_data = json.load(f)
        else:
            self.usage_data = {
                'openchargemap': {'daily': 0, 'last_reset': datetime.now().isoformat()},
                'openweather': {'daily': 0, 'last_reset': datetime.now().isoformat()},
                'here_maps': {'monthly': 0, 'last_reset': datetime.now().isoformat()}
            }
    
    def save_usage_data(self):
        """Save usage data to file"""
        os.makedirs(os.path.dirname(self.usage_file), exist_ok=True)
        with open(self.usage_file, 'w') as f:
            json.dump(self.usage_data, f, indent=2)
    
    def check_openchargemap_limit(self) -> bool:
        """Check if OpenChargeMap daily limit is reached"""
        daily_limit = 1000
        current_usage = self.usage_data['openchargemap']['daily']
        
        # Reset daily counter if new day
        last_reset = datetime.fromisoformat(self.usage_data['openchargemap']['last_reset'])
        if datetime.now().date() > last_reset.date():
            self.usage_data['openchargemap']['daily'] = 0
            self.usage_data['openchargemap']['last_reset'] = datetime.now().isoformat()
            current_usage = 0
        
        return current_usage < daily_limit
    
    def check_openweather_limit(self) -> bool:
        """Check if OpenWeatherMap daily limit is reached"""
        daily_limit = 1000
        current_usage = self.usage_data['openweather']['daily']
        
        # Reset daily counter if new day
        last_reset = datetime.fromisoformat(self.usage_data['openweather']['last_reset'])
        if datetime.now().date() > last_reset.date():
            self.usage_data['openweather']['daily'] = 0
            self.usage_data['openweather']['last_reset'] = datetime.now().isoformat()
            current_usage = 0
        
        return current_usage < daily_limit
    
    def check_here_maps_limit(self) -> bool:
        """Check if HERE Maps monthly limit is reached"""
        monthly_limit = 1000
        current_usage = self.usage_data['here_maps']['monthly']
        
        # Reset monthly counter if new month
        last_reset = datetime.fromisoformat(self.usage_data['here_maps']['last_reset'])
        if datetime.now().month != last_reset.month or datetime.now().year != last_reset.year:
            self.usage_data['here_maps']['monthly'] = 0
            self.usage_data['here_maps']['last_reset'] = datetime.now().isoformat()
            current_usage = 0
        
        return current_usage < monthly_limit
    
    def record_api_call(self, service: str, calls: int = 1):
        """Record API calls for a service"""
        if service == 'openchargemap':
            self.usage_data['openchargemap']['daily'] += calls
        elif service == 'openweather':
            self.usage_data['openweather']['daily'] += calls
        elif service == 'here_maps':
            self.usage_data['here_maps']['monthly'] += calls
        
        self.save_usage_data()
    
    def get_usage_summary(self) -> Dict:
        """Get current usage summary"""
        return {
            'openchargemap': {
                'daily_usage': self.usage_data['openchargemap']['daily'],
                'daily_limit': 1000,
                'remaining': 1000 - self.usage_data['openchargemap']['daily'],
                'can_use': self.check_openchargemap_limit()
            },
            'openweather': {
                'daily_usage': self.usage_data['openweather']['daily'],
                'daily_limit': 1000,
                'remaining': 1000 - self.usage_data['openweather']['daily'],
                'can_use': self.check_openweather_limit()
            },
            'here_maps': {
                'monthly_usage': self.usage_data['here_maps']['monthly'],
                'monthly_limit': 1000,
                'remaining': 1000 - self.usage_data['here_maps']['monthly'],
                'can_use': self.check_here_maps_limit()
            }
        }
    
    def print_usage_summary(self):
        """Print current usage summary"""
        summary = self.get_usage_summary()
        
        print("📊 Free Tier API Usage Summary")
        print("=" * 50)
        
        for service, data in summary.items():
            status = "Available" if data['can_use'] else "Limit Reached"
            print(f"{service.upper()}:")
            if 'daily_usage' in data:
                print(f"  Usage: {data['daily_usage']}/{data['daily_limit']}")
            else:
                print(f"  Usage: {data['monthly_usage']}/{data['monthly_limit']}")
            print(f"  Remaining: {data['remaining']}")
            print(f"  Status: {status}")
            print()

if __name__ == "__main__":
    monitor = FreeTierMonitor()
    monitor.print_usage_summary()
