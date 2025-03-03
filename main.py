import json
import logging
from datetime import datetime
from typing import List

import feedparser
import pyarrow as pa
import pyarrow.compute as pc
from deltalake import write_deltalake
from pyarrow import acero

LOCAL_STORAGE_PATH = "/tmp/curated_news/raw/"

STATE_FILE = "rss_state.json"

rss_feeds = {
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


def save_state(state):
    """Save the list of processed feed IDs to a JSON file."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except IOError as e:
        logger.error(f"Error saving state file: {e}")


def time_to_ts(time_struct):
    try:
        # Step 1: Convert time.struct_time to datetime.datetime
        parsed_datetime = datetime(*time_struct[:6])

        # Step 2: Convert datetime.datetime to pyarrow.timestamp
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

        if len(unprocessed_entries) == 0:
            raise EOFError(
                f"No records pending. All entries already processed according to the state file: {STATE_FILE}.")

        titles, timestamps, descriptions, links, ids, thumbnails = zip(
            *[(row['title'], time_to_ts(row['published_parsed']), row['description'], row['link'],
               row['guid'], row.get('media_thumbnail', [{'url': None}])[0]['url'])
              for row in feed.entries if row.guid not in processed_ids]
        )

        # Convert to PyArrow arrays
        t = pa.table({
            'title': pa.array(titles, type=pa.string()),
            'published_time': pa.array(timestamps, type=pa.timestamp('us')),
            'description': pa.array(descriptions, type=pa.string()),
            'link': pa.array(links, type=pa.string()),
            'id': pa.array(ids, type=pa.string()),
            'thumbnail_url': pa.array(thumbnails, type=pa.string()),
            'category': pa.array([rss_id] * len(titles), type=pa.string())
        })

        # Update the state with the newly processed IDs
        new_ids = t["id"].to_pylist()
        save_state(processed_ids + new_ids)

        return t
    except Exception as e:
        logger.error(f"Error downloading entries from {feed_url}: {e}")
        raise


def create_press_releases_sources() -> List[acero.Declaration]:
    """Create Acero declarations for each RSS feed source."""
    try:
        table_sources = []
        for rss_id, feed_url in rss_feeds.items():
            logger.info(f"Creating press releases source for {rss_id}")
            t = download_entries(rss_id, feed_url)
            table_opts = acero.TableSourceNodeOptions(t)
            table_sources.append(acero.Declaration("table_source", options=table_opts))
        return table_sources
    except Exception as e:
        logger.error(f"Error creating press releases sources: {e}")
        raise


def curate_news(ds: acero.Declaration) -> acero.Declaration:
    """
    Curate news by selecting and processing relevant fields.
    """
    try:
        logger.info("Curating news entries")
        decl = acero.Declaration.from_sequence([
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
        return decl
    except Exception as e:
        logger.error(f"Error curating news: {e}")
        raise


def store(data: pa.Table) -> None:
    """Store the processed data in Delta Lake format."""
    try:
        logger.info(f"Storing data to {LOCAL_STORAGE_PATH}")
        write_deltalake(LOCAL_STORAGE_PATH, data, mode='append')
    except Exception as e:
        logger.error(f"Error storing data: {e}")
        raise


if __name__ == "__main__":
    try:
        sources = create_press_releases_sources()
        for source in sources:
            input_table = source.to_table()
            curated_table = curate_news(source)
            store(curated_table.to_table())
        logger.info("Successfully processed all RSS feeds")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        raise
