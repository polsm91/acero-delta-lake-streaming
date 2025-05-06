"""Main script for the BBC News Analysis project."""

import logging
from pathlib import Path
from news_insights import NewsCollector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def setup_directories():
    """Create necessary directories if they don't exist."""
    from config import RAW_STORAGE_PATH, CURATED_STORAGE_PATH
    
    for path in [RAW_STORAGE_PATH, CURATED_STORAGE_PATH]:
        Path(path).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured directory exists: {path}")

def main():
    """Run the data collection process."""
    try:
        # Ensure directories exist
        setup_directories()
        
        # Collect and process data
        collector = NewsCollector()
        collector.process_feeds()
        logger.info("Data collection and processing completed successfully")
            
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
