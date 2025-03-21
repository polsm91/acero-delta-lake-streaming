# News Processor based on PyArrow Acero

Proof Of Concept to pull news data from an RSS feed, and then store them in a data lake using Delta Lake's `delta-rs` as a writer. The compute pipeline is defined using the `PyArrow Acero` library.

It will iterate over the RSS feeds, pull the data, and extract the desired fields, and store them into a Delta Lake table. To avoid processing the same RSS Feed entry twice, the ids are stored in the `rss_state.json`.

The RSS data comes from BBC's RSS feeds, explore the [BBC's page](https://www.bbc.co.uk/news/10628494). A more detailed explanation of the code [can be found here](https://pol-santamaria.com/tech/rss-feed-acero-delta-lake).

## Instructions

## Create virtual environment
```bash
python -m venv venv
source venv/bin/activate
```

##  Install dependencies
```bash
pip install -r requirements.txt
```

## Run the script
```bash
python main.py
```


## Exploring the results

Try the following in your favorite data science tool:

```python
from deltalake import DeltaTable
dt_news = DeltaTable("/tmp/curated_news/raw/")
df_news = dt_news.to_pandas()
df_news.head()
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

# Why?

The objective of the POC was to validate whether PyArrow Acero can be an efficient method to pull RSS feeds and store them for analytical purposes.

# References
* [Acero Docs](https://arrow.apache.org/docs/python/api/acero.html)

* [Acero GitHub](https://github.com/apache/arrow/)

* [Delta-rs GitHub](https://github.com/delta-io/delta-rs)
