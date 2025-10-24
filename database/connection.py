import logging
from supabase import create_client, Client
from config import Config

logger = logging.getLogger(__name__)

class DatabaseConnection:
    def __init__(self):
        self.supabase: Client = None
        self.connect()
    
    def connect(self):
        """Establish Supabase connection"""
        try:
            self.supabase = create_client(Config.SUPABASE_URL, Config.SUPABASE_KEY)
            logger.info("Supabase connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to Supabase: {str(e)}")
            raise
    
    def get_supabase(self):
        """Get Supabase client"""
        if not self.supabase:
            self.connect()
        return self.supabase
    
    def insert_data(self, table_name, data):
        """Insert data into Supabase table"""
        try:
            result = self.supabase.table(table_name).insert(data).execute()
            return result
        except Exception as e:
            logger.error(f"Failed to insert data into {table_name}: {str(e)}")
            raise
    
    def select_data(self, table_name, filters=None):
        """Select data from Supabase table"""
        try:
            query = self.supabase.table(table_name).select("*")
            if filters:
                for key, value in filters.items():
                    query = query.eq(key, value)
            result = query.execute()
            return result.data
        except Exception as e:
            logger.error(f"Failed to select data from {table_name}: {str(e)}")
            raise
    
    def execute_query(self, query, params=None):
        """Execute a SQL query (for compatibility)"""
        try:
            # For Supabase, we'll use the table methods instead
            logger.warning("execute_query not implemented for Supabase client")
            return []
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def get_session(self):
        """Get database session (for compatibility)"""
        return self
    
    def rollback(self):
        """Rollback transaction (for compatibility)"""
        # Supabase doesn't need explicit rollback
        pass
    
    def commit(self):
        """Commit transaction (for compatibility)"""
        # Supabase auto-commits
        pass
    
    def close(self):
        """Close Supabase connection"""
        # Supabase client doesn't need explicit closing
        pass

# Global database connection instance
db_connection = DatabaseConnection()
