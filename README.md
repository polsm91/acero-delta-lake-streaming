# News Insights - AI-Powered News Analysis with PyArrow Acero and Delta Lake

A proof of concept for collecting, processing, and analyzing news articles using AI and modern data technologies. Built on top of PyArrow Acerousing PyArrow Acero for streaming data processing, Delta Lake for efficient data storage, and OpenAI's GPT models for advanced text analysis.

## Key Features
- **Real-time News Collection**: Automated RSS feed collection from BBC News with extensible architecture
- **AI-Powered Analysis**: Named entity extraction using OpenAI's GPT models (actor, role, category)
- **Efficient data management**: PyArrow Acero for high-performance streaming data processing and Delta Lake for ACID-compliant data storage and time travel


## Quick Start

### 1. Environment Setup

```bash
# Create and activate virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure OpenAI Credentials

```bash
export OPENAI_API_KEY="your-api-key"
export OPENAI_ORG_ID="your-org-id"  # Optional
export OPENAI_PROJECT="your-project"  # Optional
```

### 3. Run the Pipeline

```bash
python main.py
```

### 4. Reset state (optional)

The ETL keeps track of the processed RSS entries in the JSON file `./rss_state.json`. If you want to reprocess them RSS Feeds, delete the file.


## Data Exploration

The processed data is stored in Delta Lake tables, making it easy to analyze using your favorite data science tools:

```python
from deltalake import DeltaTable

# Load news data
dt_news = DeltaTable("/tmp/bbc_news/raw/news")
df_news = dt_news.to_pandas()
df_news.head()

# Load actor data
dt_actors = DeltaTable("/tmp/bbc_news/curated/actors")
df_actors = dt_actors.to_pandas()
df_actors.head()
```

Will give you:

```python
                                               title      published_time  ...                                      thumbnail_url    category
0  Hegseth orders pause in US cyber-offensive aga... 2025-03-03 11:19:52  ...  https://ichef.bbci.co.uk/ace/standard/240/cpsp...  Technology
1    TikTok investigated over use of children's data 2025-03-03 06:10:56  ...  https://ichef.bbci.co.uk/ace/standard/240/cpsp...  Technology
2        Microsoft announces Skype will close in May 2025-02-28 17:48:12  ...  https://ichef.bbci.co.uk/ace/standard/240/cpsp...  Technology
3    WhatsApp says it has resolved technical problem 2025-02-28 18:21:37  ...  https://ichef.bbci.co.uk/ace/standard/240/cpsp...  Technology
4  Lloyds Bank says app issues fixed after payday... 2025-02-28 12:46:48  ...  https://ichef.bbci.co.uk/ace/standard/240/cpsp...  Technology
```

```python
                                            news_id actor_name                   actor_role  is_main_actor
0  https://www.bbc.com/news/articles/cp913ze3k9jo#0  Microsoft  company shutting down Skype           True
1  https://www.bbc.com/news/articles/crkx3vy54nzo#0      Co-op             affected company           True
2  https://www.bbc.com/news/articles/crkx3vy54nzo#0        BBC         contacted by hackers          False
3  https://www.bbc.com/news/articles/c9856ge2742o#0      Co-op                     retailer           True
4  https://www.bbc.com/news/articles/c86jx18y9e2o#0      Apple             technology giant           True
```



## Project Structure

```
news_insights/
├── __init__.py      # Module exports and versioning
├── processor.py     # NewsProcessor class for AI analysis
└── collector.py     # NewsCollector class for RSS feed handling
```


## How it works


1. **News Collection**: The `NewsCollector` class fetches news articles from BBC RSS feeds
2. **Data Processing**: Uses PyArrow Acero for efficient data processing
3. **AI Analysis**: The `NewsProcessor` class uses OpenAI's GPT models to:
   - Extract named entities (actors)
   - Identify their roles
   - Classify events
4. **Storage**: Results are stored in Delta Lake tables for efficient querying



## Current Limitations

Even if it is a proof of concept, there are several areas for improvement:

- **High cardinality of roles**: The model needs refinement regarding categories, which are not used right now, or roles.
- **Process data in a streaming fashion**: Acero is a streaming engine but requires an orchestrator to keep the tasks running in a loop, and to monitor them (e.g. Dagster, Prefect, Airflow, ...)
- **RSS State**: We keep track of the processed entities in a JSON file. A proper database with fast lookups should be used.
- **Optimize entity extraction**: The entities are extracted sequentially, with several transformations (*OpenAI JSON -> PyDantic -> List -> PyArrow Table*). We can make concurrent calls to OpenAI and reduce the transformations.

## References

* [Acero Documentation](https://arrow.apache.org/docs/python/api/acero.html)
* [Acero GitHub Repository](https://github.com/apache/arrow/)
* [Delta Lake Documentation](https://github.com/delta-io/delta-rs)
* [OpenAI API Documentation](https://platform.openai.com/docs/api-reference)
* [BBC News RSS Feeds](https://www.bbc.co.uk/news/10628494)

## License

This project is licensed under the Apache 2.0 License - see the LICENSE file for details.
