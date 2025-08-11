"""
Database Connector Factory for UX Study
Creates appropriate database connectors based on configuration
"""

import logging
from typing import Optional

from database.base_connector import BaseDatabaseConnector
from database.spanner_connector import SpannerConnector

logger = logging.getLogger(__name__)


def create_study_connector() -> BaseDatabaseConnector:
    """
    Create a database connector for the UX study
    
    Returns:
        BaseDatabaseConnector: A configured database connector
    """
    try:
        # For now, return the Spanner connector
        # This can be extended to support different database types
        connector = SpannerConnector()
        logger.info(f"Created {connector.get_provider_name()} connector")
        return connector
    except Exception as e:
        logger.error(f"Failed to create database connector: {str(e)}")
        raise


def get_connector_by_type(connector_type: str) -> Optional[BaseDatabaseConnector]:
    """
    Get a specific type of database connector
    
    Args:
        connector_type: Type of connector to create
        
    Returns:
        BaseDatabaseConnector or None if type not supported
    """
    if connector_type.lower() == "spanner":
        return SpannerConnector()
    # Add other connector types here as needed
    
    logger.warning(f"Unsupported connector type: {connector_type}")
    return None
