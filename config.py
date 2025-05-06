"""Configuration settings for the BBC News Analysis project."""

# Storage paths
RAW_STORAGE_PATH = "/tmp/bbc_news/raw/"
CURATED_STORAGE_PATH = "/tmp/bbc_news/curated/"

# Delta Lake table names
NEWS_TABLE = "news"
ACTORS_TABLE = "actors"

# Full paths for Delta Lake tables
RAW_NEWS_PATH = RAW_STORAGE_PATH + NEWS_TABLE
CURATED_NEWS_PATH = CURATED_STORAGE_PATH + NEWS_TABLE
CURATED_ACTORS_PATH = CURATED_STORAGE_PATH + ACTORS_TABLE 
