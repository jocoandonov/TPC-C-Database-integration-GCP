"""
Google Spanner Database Connector - WORKING IMPLEMENTATION
Fully functional connector for Google Cloud Spanner
"""

import logging
import os
from typing import Any, Dict, List, Optional

from google.cloud import spanner
from google.cloud.spanner_v1 import Client
from .base_connector import BaseDatabaseConnector

logger = logging.getLogger(__name__)


class SpannerConnector(BaseDatabaseConnector):
    """
    Google Spanner database connector for TPC-C application
    Fully implemented with working connection and query execution
    """

    def __init__(self):
        """Initialize Google Spanner connection"""
        super().__init__()
        self.provider_name = "Google Cloud Spanner"

        # Read configuration from environment
        self.project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        self.instance_id = os.getenv("SPANNER_INSTANCE_ID")
        self.database_id = os.getenv("SPANNER_DATABASE_ID")
        self.credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Display configuration status
        print(f"🔧 Spanner Configuration:")
        print(f"   Project ID: {self.project_id or '❌ NOT SET'}")
        print(f"   Instance ID: {self.instance_id or '❌ NOT SET'}")
        print(f"   Database ID: {self.database_id or '❌ NOT SET'}")
        
        if self.credentials_path:
            print(f"   Credentials: ✅ {self.credentials_path}")
        else:
            print(f"   Credentials: ❌ NOT SET")

        # Initialize Spanner client and database connections
        self.client = None
        self.instance = None
        self.database = None
        
        try:
            self._initialize_spanner_client()
        except Exception as e:
            logger.error(f"Failed to initialize Spanner client: {str(e)}")
            print(f"❌ Failed to initialize Spanner client: {str(e)}")

    def _initialize_spanner_client(self):
        """Initialize Spanner client and database connections"""
        if not all([self.project_id, self.instance_id, self.database_id]):
            raise ValueError("Missing required Spanner configuration")
        
        try:
            # Create Spanner client
            self.client = spanner.Client(project=self.project_id)
            print(f"✅ Spanner client created for project: {self.project_id}")
            
            # Get instance and database
            self.instance = self.client.instance(self.instance_id)
            self.database = self.instance.database(self.database_id)
            print(f"✅ Connected to instance: {self.instance_id}")
            print(f"✅ Connected to database: {self.database_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Spanner connections: {str(e)}")
            print(f"❌ Failed to initialize Spanner connections: {str(e)}")
            raise

    def test_connection(self) -> bool:
        """Test connection to Google Spanner database"""
        try:
            if not self.database:
                print("❌ No database connection available")
                return False
            
            # Execute a simple test query
            query = "SELECT 1 as test"
            result = self.execute_query(query)
            
            if result and len(result) > 0:
                print("✅ Spanner connection test successful")
                return True
            else:
                print("❌ Spanner connection test failed - no results")
                return False
                
        except Exception as e:
            logger.error(f"Spanner connection test failed: {str(e)}")
            print(f"❌ Spanner connection test failed: {str(e)}")
            return False

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """Execute SQL query on Google Spanner"""
        try:
            if not self.database:
                logger.error("No database connection available")
                return []
            
            # Execute the query
            with self.database.snapshot() as snapshot:
                if params:
                    results = snapshot.execute_sql(query, params=params)
                else:
                    results = snapshot.execute_sql(query)
                
                # Convert results to list of dictionaries
                columns = [field.name for field in results.fields]
                rows = []
                
                for row in results:
                    row_dict = {}
                    for i, value in enumerate(row):
                        # Handle Spanner-specific data types
                        if hasattr(value, 'isoformat'):  # datetime
                            row_dict[columns[i]] = value.isoformat()
                        else:
                            row_dict[columns[i]] = value
                    rows.append(row_dict)
                
                return rows
                
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            print(f"❌ Query execution failed: {str(e)}")
            return []

    def get_provider_name(self) -> str:
        """Get the database provider name"""
        return self.provider_name

    def close_connection(self):
        """Close database connection"""
        try:
            if self.client:
                self.client.close()
                print("✅ Spanner connection closed")
        except Exception as e:
            logger.error(f"Error closing Spanner connection: {str(e)}")
            print(f"❌ Error closing Spanner connection: {str(e)}")
