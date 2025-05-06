"""Main script for the BBC News Analysis project."""

import logging
from news_insights import NewsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    try:
        collector = NewsCollector()
        collector.process_feeds()
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
