"""
Interactive COVID-19 Dashboard using Streamlit.
Visualizes data from the ETL pipeline.
"""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yaml
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from load.data_loader import DataLoader # noqa

# Page configuration
st.set_page_config(
    page_title="COVID-19 Analytics Dashboard",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS
st.markdown(
    """
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        background: linear-gradient(120deg, #d32f2f, #f57c00);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 10px;
        border-left: 4px solid #d32f2f;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    </style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def load_config():
    """Load configuration."""
    config_path = Path(__file__).parent.parent.parent / "config" / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@st.cache_resource
def get_loader(config):
    """Get database loader."""
    return DataLoader(config)


@st.cache_data(ttl=3600)
def load_global_stats(_loader):
    """Load global statistics."""
    query = """
        SELECT
            date,
            global_new_cases,
            global_new_deaths,
            global_total_cases,
            global_total_deaths,
            global_new_cases_7day_avg,
            global_new_deaths_7day_avg
        FROM global_daily_stats
        ORDER BY date DESC
    """
    return _loader.query(query)


@st.cache_data(ttl=3600)
def load_country_stats(_loader):
    """Load country aggregated statistics."""
    query = """
        SELECT
            location,
            total_cases,
            total_deaths,
            cases_per_100k,
            deaths_per_100k,
            avg_case_fatality_rate,
            population
        FROM aggregated_stats
        ORDER BY total_cases DESC
        LIMIT 50
    """
    return _loader.query(query)


@st.cache_data(ttl=3600)
def load_country_timeline(_loader, countries):
    """Load timeline data for specific countries."""
    countries_str = "', '".join(countries)
    query = f"""
        SELECT
            location,
            date,
            new_cases,
            new_deaths,
            total_cases,
            total_deaths
        FROM covid_cases
        WHERE location IN ('{countries_str}')
        ORDER BY date ASC
    """
    return _loader.query(query)


@st.cache_data(ttl=3600)
def load_vaccination_data(_loader, countries):
    """Load vaccination data for specific countries."""
    countries_str = "', '".join(countries)
    query = f"""
        SELECT
            location,
            date,
            total_vaccinations,
            people_vaccinated,
            people_fully_vaccinated,
            daily_vaccinations
        FROM vaccinations
        WHERE location IN ('{countries_str}')
        ORDER BY date ASC
    """
    return _loader.query(query)


def create_global_trend_chart(df):
    """Create global trend chart."""
    fig = make_subplots(
        rows=2,
        cols=1,
        subplot_titles=("Daily New Cases (7-day avg)", "Daily New Deaths (7-day avg)"),
        vertical_spacing=0.12,
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["global_new_cases_7day_avg"],
            name="New Cases",
            line=dict(color="#FF6B6B", width=2),
            fill="tozeroy",
            fillcolor="rgba(255, 107, 107, 0.2)",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["global_new_deaths_7day_avg"],
            name="New Deaths",
            line=dict(color="#4D4D4D", width=2),
            fill="tozeroy",
            fillcolor="rgba(77, 77, 77, 0.2)",
        ),
        row=2,
        col=1,
    )

    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_yaxes(title_text="Cases", row=1, col=1)
    fig.update_yaxes(title_text="Deaths", row=2, col=1)

    fig.update_layout(height=600, showlegend=False, hovermode="x unified")

    return fig


def create_top_countries_chart(df, metric="total_cases", top_n=15):
    """Create bar chart for top countries."""
    df_sorted = df.nlargest(top_n, metric)

    colors = ["#d32f2f" if metric == "total_cases" else "#4D4D4D"] * top_n

    fig = go.Figure(
        data=[
            go.Bar(
                x=df_sorted[metric],
                y=df_sorted["location"],
                orientation="h",
                marker_color=colors,
                text=df_sorted[metric].apply(
                    lambda x: f"{x/1e6:.2f}M" if x > 1e6 else f"{x/1e3:.1f}K"
                ),
                textposition="auto",
            )
        ]
    )

    title = f'Top {top_n} Countries - {metric.replace("_", " ").title()}'

    fig.update_layout(
        title=title,
        xaxis_title=metric.replace("_", " ").title(),
        yaxis_title="Country",
        height=500,
        showlegend=False,
    )

    return fig


def create_country_comparison_chart(df, countries, metric="new_cases"):
    """Create line chart comparing countries."""
    fig = px.line(
        df,
        x="date",
        y=metric,
        color="location",
        title=f'{metric.replace("_", " ").title()} - Country Comparison',
        labels={metric: metric.replace("_", " ").title(), "date": "Date"},
    )

    fig.update_layout(
        height=500,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def create_vaccination_progress_chart(df, countries):
    """Create vaccination progress chart."""
    fig = px.line(
        df,
        x="date",
        y="people_fully_vaccinated",
        color="location",
        title="Vaccination Progress - Fully Vaccinated People",
        labels={"people_fully_vaccinated": "People Fully Vaccinated", "date": "Date"},
    )

    fig.update_layout(
        height=500,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


def main():
    """Main dashboard application."""

    # Header
    st.markdown(
        '<div class="main-header">🦠 COVID-19 Analytics Dashboard</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <div style='text-align: center; color: #666; margin-bottom: 2rem;'>
    Global COVID-19 data analysis powered by PySpark ETL Pipeline
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Load configuration and data
    config = load_config()
    loader = get_loader(config)

    # Sidebar
    st.sidebar.title("⚙️ Dashboard Controls")
    st.sidebar.markdown("---")

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.sidebar.markdown("---")

    # Load data
    with st.spinner("📊 Loading data from database..."):
        global_stats = load_global_stats(loader)
        country_stats = load_country_stats(loader)

    if global_stats.empty:
        st.error("❌ No data found in database. Please run the ETL pipeline first.")
        st.stop()

    # Global Summary Metrics
    st.markdown("### 🌍 Global Statistics")

    # Get latest statistics WITHOUT NaN
    valid_data = global_stats[
        global_stats['global_total_cases'].notna() & 
        global_stats['global_total_deaths'].notna()
    ]

    if valid_data.empty:
        st.error("❌ No valid data found")
        st.stop()

    latest = valid_data.iloc[0]

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "📈 Total Cases",
            f"{latest['global_total_cases']/1e6:.1f}M",
            f"+{latest['global_new_cases']/1e3:.1f}K today",
        )

    with col2:
        st.metric(
            "💀 Total Deaths",
            f"{latest['global_total_deaths']/1e6:.2f}M",
            f"+{latest['global_new_deaths']:.0f} today",
        )

    with col3:
        fatality_rate = (
            latest["global_total_deaths"] / latest["global_total_cases"]
        ) * 100
        st.metric("📊 Fatality Rate", f"{fatality_rate:.2f}%")

    with col4:
        st.metric("🌐 Countries", f"{len(country_stats)}", "reporting data")

    st.markdown("---")

    # Global Trends
    st.markdown("### 📈 Global Trends")

    # Date range filter
    col1, col2 = st.columns([3, 1])
    with col1:
        days_back = st.slider(
            "Show last N days:",
            min_value=30,
            max_value=len(global_stats),
            value=min(180, len(global_stats)),
            step=30,
        )

    filtered_global = global_stats.head(days_back).sort_values("date")

    trend_chart = create_global_trend_chart(filtered_global)
    st.plotly_chart(trend_chart, use_container_width=True)

    st.markdown("---")

    # Top Countries Analysis
    st.markdown("### 🏆 Top Countries")

    tab1, tab2, tab3 = st.tabs(["Total Cases", "Total Deaths", "Cases per 100K"])

    with tab1:
        chart = create_top_countries_chart(country_stats, "total_cases")
        st.plotly_chart(chart, use_container_width=True)

    with tab2:
        chart = create_top_countries_chart(country_stats, "total_deaths")
        st.plotly_chart(chart, use_container_width=True)

    with tab3:
        chart = create_top_countries_chart(country_stats, "cases_per_100k")
        st.plotly_chart(chart, use_container_width=True)

    st.markdown("---")

    # Country Comparison
    st.markdown("### 🔍 Country Comparison")

    available_countries = config["processing"]["countries_of_interest"]

    selected_countries = st.multiselect(
        "Select countries to compare:",
        available_countries,
        default=available_countries[:5],
    )

    if selected_countries:
        # Load timeline data
        timeline_data = load_country_timeline(loader, selected_countries)

        if not timeline_data.empty:
            tab1, tab2, tab3 = st.tabs(
                ["Daily Cases", "Daily Deaths", "Cumulative Cases"]
            )

            with tab1:
                chart = create_country_comparison_chart(
                    timeline_data, selected_countries, "new_cases"
                )
                st.plotly_chart(chart, use_container_width=True)

            with tab2:
                chart = create_country_comparison_chart(
                    timeline_data, selected_countries, "new_deaths"
                )
                st.plotly_chart(chart, use_container_width=True)

            with tab3:
                chart = create_country_comparison_chart(
                    timeline_data, selected_countries, "total_cases"
                )
                st.plotly_chart(chart, use_container_width=True)

        # Vaccination Data
        st.markdown("### 💉 Vaccination Progress")

        vacc_data = load_vaccination_data(loader, selected_countries)

        if not vacc_data.empty:
            vacc_chart = create_vaccination_progress_chart(
                vacc_data, selected_countries
            )
            st.plotly_chart(vacc_chart, use_container_width=True)
        else:
            st.info("💉 Vaccination data not available for selected countries")

    # Data Table
    with st.expander("📋 View Country Statistics Table"):
        st.dataframe(
            country_stats[
                [
                    "location",
                    "total_cases",
                    "total_deaths",
                    "cases_per_100k",
                    "deaths_per_100k",
                    "avg_case_fatality_rate",
                ]
            ].head(30),
            use_container_width=True,
        )

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666; padding: 2rem 0;'>
            <strong>Data Source:</strong> Our World in Data<br>
            <strong>Pipeline:</strong> PySpark ETL | SQLite Database<br>
            <em>Data updated daily via automated Airflow pipeline</em>
        </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
