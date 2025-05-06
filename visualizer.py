import streamlit as st
import pandas as pd
import plotly.express as px
from deltalake import DeltaTable
import pyarrow as pa
import logging
from pathlib import Path
from config import CURATED_NEWS_PATH, CURATED_ACTORS_PATH

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_page():
    """Initialize the Streamlit page configuration."""
    try:
        st.set_page_config(
            page_title="BBC News Analysis Dashboard",
            page_icon="📰",
            layout="wide"
        )
    except Exception as e:
        logger.warning(f"Page config already set: {e}")

# Load data from Delta Lake tables
@st.cache_data(ttl=3600)  # Cache for 1 hour
def load_data():
    """Load data from Delta Lake tables with error handling."""
    try:
        # Verify paths exist
        if not Path(CURATED_NEWS_PATH).exists():
            raise FileNotFoundError(f"News data not found at {CURATED_NEWS_PATH}")
        if not Path(CURATED_ACTORS_PATH).exists():
            raise FileNotFoundError(f"Actors data not found at {CURATED_ACTORS_PATH}")

        # Load news data
        news_table = DeltaTable(CURATED_NEWS_PATH)
        news_df = news_table.to_pandas()
        
        # Load actors data
        actors_table = DeltaTable(CURATED_ACTORS_PATH)
        actors_df = actors_table.to_pandas()
        
        return news_df, actors_df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        st.error(f"Error loading data: {e}")
        return pd.DataFrame(), pd.DataFrame()

def main():
    """Main function to run the Streamlit app."""
    try:
        # Initialize page
        init_page()

        # Load the data
        news_df, actors_df = load_data()

        if news_df.empty or actors_df.empty:
            st.error("No data available. Please run the data collection script first.")
            return

        # Sidebar filters
        st.sidebar.title("Filters")

        # Category filter
        selected_categories = st.sidebar.multiselect(
            "News Categories",
            options=news_df['category'].unique(),
            default=news_df['category'].unique()
        )

        # Date range filter
        min_date = news_df['published_time'].min()
        max_date = news_df['published_time'].max()
        selected_dates = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )

        # Filter data
        filtered_news = news_df[
            (news_df['category'].isin(selected_categories)) &
            (news_df['published_time'].dt.date >= selected_dates[0]) &
            (news_df['published_time'].dt.date <= selected_dates[1])
        ]

        filtered_actors = actors_df[actors_df['news_id'].isin(filtered_news['id'])]
        
        # Filter out reporters and BBC
        filtered_actors = filtered_actors[
            (~filtered_actors['actor_name'].str.contains('BBC', case=False, na=False)) &
            (~filtered_actors['actor_role'].str.contains('reporter', case=False, na=False))
        ]

        # Header
        st.title("📰 BBC News Analysis Dashboard")

        # Create three columns for metrics
        col1, col2, col3 = st.columns(3)

        with col1:
            st.metric("Total Articles", len(filtered_news))
        with col2:
            st.metric("Unique Actors", len(filtered_actors['actor_name'].unique()))
        with col3:
            st.metric("Main Actors", len(filtered_actors[filtered_actors['is_main_actor']]))

        # Compact timeline at the top
        timeline_data = filtered_news.groupby(filtered_news['published_time'].dt.date).size().reset_index(name='count')
        fig4 = px.line(
            timeline_data,
            x='published_time',
            y='count',
            title='News Articles Over Time',
            labels={'published_time': 'Date', 'count': 'Number of Articles'}
        )
        fig4.update_layout(
            height=200,  # Make the chart shorter
            margin=dict(l=20, r=20, t=40, b=20),  # Reduce margins
            showlegend=False,  # Remove legend
            xaxis_title=None,  # Remove x-axis title
            yaxis_title=None,  # Remove y-axis title
        )
        st.plotly_chart(fig4, use_container_width=True)

        # 1. Actor Analysis
        st.subheader("👤 Actor Analysis")

        # Get top 10 most frequent actors
        top_actors = filtered_actors['actor_name'].value_counts().head(10).index
        
        # Get role distribution for top actors
        top_actors_roles = filtered_actors[filtered_actors['actor_name'].isin(top_actors)]
        role_counts = top_actors_roles.groupby(['actor_name', 'actor_role']).size().reset_index(name='count')
        
        # Create a stacked bar chart showing roles for each top actor
        fig3 = px.bar(
            role_counts,
            x='actor_name',
            y='count',
            color='actor_role',
            title='Roles of Top 10 Most Mentioned Actors',
            labels={'count': 'Number of Mentions', 'actor_name': 'Actor', 'actor_role': 'Role'}
        )
        # Sort by total mentions
        fig3.update_layout(
            xaxis={'categoryorder': 'total descending'},
            barmode='stack'
        )
        st.plotly_chart(fig3, use_container_width=True)

        # 2. News Category Distribution
        st.subheader("🗂️ News Category Distribution")
        category_count = filtered_news['category'].value_counts().reset_index()
        category_count.columns = ['Category', 'Count']
        fig = px.pie(
            category_count,
            values='Count',
            names='Category',
            title='Distribution of News Categories'
        )
        st.plotly_chart(fig, use_container_width=True)

        # 4. Detailed View
        st.subheader("📄 News Articles")
        # Create an expandable section for the detailed view
        with st.expander("View Detailed News Data"):
            # Show news articles with their actors
            detailed_view = filtered_news.merge(
                filtered_actors,
                left_on='id',
                right_on='news_id',
                how='left'
            )
            st.dataframe(
                detailed_view[['title', 'category', 'published_time', 'actor_name', 'actor_role', 'is_main_actor']],
                use_container_width=True
            )

    except Exception as e:
        logger.error(f"Error in main execution: {e}")
        st.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

