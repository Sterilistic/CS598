#!/usr/bin/env python3
"""
Script to create Tableau views in Supabase database
Run this script to set up all the views for Tableau visualization
"""

import logging
import sys
from pathlib import Path
from database.connection import db_connection
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def read_sql_file(file_path: str) -> str:
    """Read SQL file content"""
    try:
        with open(file_path, 'r') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading SQL file: {str(e)}")
        raise

def execute_sql_statements(sql_content: str):
    """Execute SQL statements in Supabase"""
    try:
        supabase = db_connection.get_supabase()
        
        # Split SQL content by semicolons to get individual statements
        # Remove comments and empty statements
        statements = []
        current_statement = []
        
        for line in sql_content.split('\n'):
            line = line.strip()
            # Skip empty lines and comments
            if not line or line.startswith('--'):
                continue
            
            current_statement.append(line)
            
            # If line ends with semicolon, we have a complete statement
            if line.endswith(';'):
                statement = ' '.join(current_statement)
                if statement.strip() and not statement.strip().startswith('--'):
                    statements.append(statement)
                current_statement = []
        
        # Execute each statement
        for i, statement in enumerate(statements, 1):
            try:
                logger.info(f"Executing statement {i}/{len(statements)}...")
                # Supabase Python client doesn't support raw SQL execution directly
                # We need to use the REST API or psycopg2
                # For now, we'll use the REST API approach
                result = supabase.rpc('exec_sql', {'query': statement}).execute()
                logger.info(f"✓ Statement {i} executed successfully")
            except Exception as e:
                # If RPC doesn't exist, we'll need to use direct connection
                logger.warning(f"RPC method not available, trying alternative approach: {str(e)}")
                # Alternative: Use psycopg2 for direct SQL execution
                try:
                    import psycopg2
                    from urllib.parse import urlparse
                    
                    # Parse Supabase URL to get connection details
                    # Supabase connection string format: postgresql://postgres:[password]@[host]:[port]/postgres
                    # We'll need to use the direct PostgreSQL connection
                    logger.info("Using direct PostgreSQL connection...")
                    # Note: You'll need to set up direct PostgreSQL connection string
                    # This is a placeholder - you'll need to configure this
                    logger.warning("Direct SQL execution requires PostgreSQL connection string")
                    logger.info("Please run the SQL file directly in Supabase SQL Editor")
                    return False
                except ImportError:
                    logger.error("psycopg2 not available for direct SQL execution")
                    return False
        
        return True
        
    except Exception as e:
        logger.error(f"Error executing SQL statements: {str(e)}")
        return False

def main():
    """Main function"""
    logger.info("Starting Tableau views creation...")
    
    # Get SQL file path
    sql_file = Path(__file__).parent.parent / 'database' / 'tableau_views.sql'
    
    if not sql_file.exists():
        logger.error(f"SQL file not found: {sql_file}")
        sys.exit(1)
    
    # Read SQL file
    logger.info(f"Reading SQL file: {sql_file}")
    sql_content = read_sql_file(str(sql_file))
    
    # Note: Supabase Python client doesn't support raw SQL execution
    # The best approach is to provide instructions for manual execution
    # or use the Supabase SQL Editor
    
    logger.info("\n" + "="*60)
    logger.info("IMPORTANT: Supabase Python client doesn't support raw SQL execution")
    logger.info("="*60)
    logger.info("\nTo create the views, you have two options:")
    logger.info("\nOption 1: Use Supabase SQL Editor (Recommended)")
    logger.info("1. Go to your Supabase dashboard")
    logger.info(f"2. Navigate to SQL Editor")
    logger.info("3. Copy and paste the contents of: database/tableau_views.sql")
    logger.info("4. Execute the SQL")
    
    logger.info("\nOption 2: Use psql command line")
    logger.info("1. Get your Supabase connection string from dashboard")
    logger.info("2. Run: psql [connection_string] < database/tableau_views.sql")
    
    logger.info("\n" + "="*60)
    logger.info(f"\nSQL file location: {sql_file}")
    logger.info("="*60 + "\n")
    
    # Print first few lines of SQL for verification
    lines = sql_content.split('\n')[:10]
    logger.info("First few lines of SQL file:")
    for line in lines:
        logger.info(f"  {line}")

if __name__ == "__main__":
    main()

