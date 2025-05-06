import json
import logging
from datetime import datetime
from typing import List, Dict

import feedparser
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake
from pyarrow import acero
from news_processor import NewsProcessor

# Configuration
RAW_STORAGE_PATH:str = "/tmp/bbc_news/raw/"
CURATED_STORAGE_PATH:str = "/tmp/bbc_news/curated/"
STATE_FILE:str = "rss_state.json"

# RSS Feed configuration
RSS_FEEDS: Dict[str, str] = {
    "Business": "http://feeds.bbci.co.uk/news/business/rss.xml",
    "Health": "http://feeds.bbci.co.uk/news/health/rss.xml",
    "Politics": "http://feeds.bbci.co.uk/news/politics/rss.xml",
    "Science": "http://feeds.bbci.co.uk/news/science_and_environment/rss.xml",
    "Technology": "http://feeds.bbci.co.uk/news/technology/rss.xml"
}

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_state():
    """Load the list of processed feed IDs from a JSON file."""
    try:
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"State file {STATE_FILE} not found. Creating new state.")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding state file: {e}")
        return []


def save_state(state: List[str]) -> None:
    """Save the list of processed feed IDs to a JSON file."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except IOError as e:
        logger.error(f"Error saving state file: {e}")


def time_to_ts(time_struct) -> pa.TimestampScalar:
    """Convert time.struct_time to pyarrow.timestamp."""
    try:
        parsed_datetime = datetime(*time_struct[:6])
        return pa.array([parsed_datetime], type=pa.timestamp('us'))[0]
    except Exception as e:
        logger.error(f"Error converting timestamp: {e}")
        raise


def download_entries(rss_id: str, feed_url: str) -> pa.Table:
    """
    Download and process RSS feed entries, returning a PyArrow table.
    
    Args:
        rss_id: The category ID of the RSS feed
        feed_url: The URL of the RSS feed
        
    Returns:
        PyArrow table containing the processed entries
    """
    try:
        logger.info(f"Fetching RSS feed from {feed_url}")
        feed = feedparser.parse(feed_url)
        processed_ids = load_state()

        unprocessed_entries = [row for row in feed.entries if row.guid not in processed_ids]

        if not unprocessed_entries:
            raise EOFError(
                f"No records pending. All entries already processed according to the state file: {STATE_FILE}.")

        # Process entries
        titles, timestamps, descriptions, links, ids, thumbnails = zip(
            *[(row['title'], time_to_ts(row['published_parsed']), row['description'], row['link'],
               row['guid'], row.get('media_thumbnail', [{'url': None}])[0]['url'])
              for row in unprocessed_entries]
        )

        # Create PyArrow table
        table = pa.table({
            'title': pa.array(titles, type=pa.string()),
            'published_time': pa.array(timestamps, type=pa.timestamp('us')),
            'description': pa.array(descriptions, type=pa.string()),
            'link': pa.array(links, type=pa.string()),
            'id': pa.array(ids, type=pa.string()),
            'thumbnail_url': pa.array(thumbnails, type=pa.string()),
            'category': pa.array([rss_id] * len(titles), type=pa.string())
        })

        # Update state
        new_ids = table["id"].to_pylist()
        save_state(processed_ids + new_ids)

        return table
    except Exception as e:
        logger.error(f"Error downloading entries from {feed_url}: {e}")
        raise


def create_press_releases_sources() -> List[acero.Declaration]:
    """Create Table Source declarations for each RSS feed source."""
    try:
        table_sources = []
        for rss_id, feed_url in RSS_FEEDS.items():
            logger.info(f"Creating press releases source for {rss_id}")
            t = download_entries(rss_id, feed_url)
            table_opts = acero.TableSourceNodeOptions(t)
            table_sources.append(acero.Declaration("table_source", options=table_opts))
        return table_sources
    except Exception as e:
        logger.error(f"Error creating press releases sources: {e}")
        raise


def curate_news(ds: acero.Declaration) -> acero.Declaration:
    """Curate news by selecting and processing relevant fields."""
    try:
        logger.info("Curating news entries")
        return acero.Declaration.from_sequence([
            ds,
            acero.Declaration("project", acero.ProjectNodeOptions([
                pc.field("title"),
                pc.field("published_time"),
                pc.field("description"),
                pc.field("link"),
                pc.field("id"),
                pc.field("thumbnail_url"),
                pc.field("category")
            ]))
        ])
    except Exception as e:
        logger.error(f"Error curating news: {e}")
        raise


def extract_actors(table: pa.Table, news_processor: NewsProcessor) -> pa.Table:
    """
    Extract actors from news articles and create a new table with actor information.
    
    Args:
        table: PyArrow table containing news articles with title and description columns
        
    Returns:
        PyArrow table containing actor information with columns:
        - news_id: ID of the news article
        - actor_name: Name of the actor
        - actor_role: Role of the actor
        - is_main_actor: Boolean indicating if this is a main actor
    """
    try:
        logger.info("Extracting actors from news articles")
        
        # Lists to store the flattened actor information
        news_ids = []
        actor_names = []
        actor_roles = []
        is_main_actor = []
        
        # Extract actors from each article
        for news_id, title, description in zip(
            table["id"].to_pylist(),
            table["title"].to_pylist(),
            table["description"].to_pylist()
        ):
            try:
                # Combine title and description for better context
                text = f"{title}\n{description}"
                result = news_processor.analyze_text(text)
                
                if result is None:
                    logger.warning(f"Failed to extract actors for news_id {news_id}")
                    continue
                
                # Process main actors
                for actor in result.main_actors:
                    news_ids.append(news_id)
                    actor_names.append(actor.name)
                    actor_roles.append(actor.role)
                    is_main_actor.append(True)
                
                # Process other actors
                for actor in result.other_actors:
                    news_ids.append(news_id)
                    actor_names.append(actor.name)
                    actor_roles.append(actor.role)
                    is_main_actor.append(False)
                    
            except Exception as e:
                logger.error(f"Error extracting actors for news_id {news_id}: {e}")
                continue
        
        # Create a new table with actor information
        return pa.table({
            'news_id': pa.array(news_ids, type=pa.string()),
            'actor_name': pa.array(actor_names, type=pa.string()),
            'actor_role': pa.array(actor_roles, type=pa.string()),
            'is_main_actor': pa.array(is_main_actor, type=pa.bool_())
        })
        
    except Exception as e:
        logger.error(f"Error in actor extraction: {e}")
        raise


def store(data: pa.Table, storage_path: str) -> None:
    """Store the processed data in Delta Lake format."""
    try:
        logger.info(f"Storing data to {storage_path}")
        write_deltalake(storage_path, data, mode='append')
    except Exception as e:
        logger.error(f"Error storing data: {e}")
        raise


if __name__ == "__main__":
    news_processor = NewsProcessor()
    try:
        sources = create_press_releases_sources()
        for source in sources:
            input_table = source.to_table()
            curated_table = curate_news(source).to_table()
            store(curated_table, RAW_STORAGE_PATH + "curated_news")
            
            # Extract actors from the curated table
            actors_table = extract_actors(curated_table, news_processor)
            store(actors_table, CURATED_STORAGE_PATH + "actors")
            
        logger.info("Successfully processed all RSS feeds")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
