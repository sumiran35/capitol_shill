import streamlit as st
import pandas as pd
import plotly.express as px
from src.data_store import sync_data, load_local_data
from src.enrichment.asset_metadata import AssetEnricher

st.set_page_config(
    page_title="Capitol Shill",
    page_icon="$ ^ _ ^ $",
    layout="wide",
    initial_sidebar_state="expanded",
)


# Cache this so we don't re-enrich on every UI click
@st.cache_data(ttl=3600)
def get_data_pipeline():
    # 1. Sync Data (Scrape & Append)
    # Try to update, fallback to local if scraper fails (e.g. no internet)
    try:
        df = sync_data()
    except Exception as e:
        st.error(f"Scraper caught an L: {e}")
        df = load_local_data()

    if df.empty:
        return pd.DataFrame()

    # 2. Enrich (Yahoo Finance)
    # We only enrich rows that are missing sectors (saves time)
    if 'sector' not in df.columns or df['sector'].isnull().any():
        enricher = AssetEnricher()
        df = enricher.enrich_dataframe(df)

    return df


def main():
    st.title("Capitol Shill")
    st.markdown('### Live trading surveillance (NSA type shit)###')
    st.markdown('Monitoring real-time stock Stright from capitol hill')
    st.divider()

    # Load Data
    with st.spinner('Fetchin data.'):
        df = get_data_pipeline()

    if df.empty:
        st.error("Data ghosted us. Run the scraper manually or check connection.")
        st.stop()

    # --- Header Metrics ---
    # Determine freshness
    last_trade = df['transaction_date'].max()
    is_fresh = (pd.Timestamp.now() - last_trade).days < 7
    status_color = "green" if is_fresh else "red"
    status_text = "Fresh 🟢" if is_fresh else "Stale 🔴"

    c1, c2, c3 = st.columns(3)
    c1.metric("Database Status", status_text)
    c2.metric("Last Trade Date", last_trade.strftime('%Y-%m-%d'))
    c3.metric("Total Records", len(df))

    # --- Filters ---
    st.sidebar.header("Filter Trades")

    # Senator filter
    all_senators = sorted(df['senator'].dropna().unique().tolist())
    selected_senators = st.sidebar.multiselect("Senator", all_senators)

    # Sector filters
    if 'sector' in df.columns:
        # Filter out 'Unknown' or nans for the dropdown
        clean_sectors = [str(s) for s in df['sector'].unique() if s and str(s) != 'nan']
        all_sectors = sorted(clean_sectors)
        selected_sectors = st.sidebar.multiselect("Sector", all_sectors)
    else:
        selected_sectors = []

    # Ticker filter
    all_tickers = sorted(df['ticker'].dropna().unique().tolist())
    selected_ticker = st.sidebar.selectbox("Specific Ticker", ["All"] + all_tickers)

    # Apply filters
    filtered_df = df.copy()

    if selected_senators:
        filtered_df = filtered_df[filtered_df['senator'].isin(selected_senators)]
    if selected_sectors:
        filtered_df = filtered_df[filtered_df['sector'].isin(selected_sectors)]
    if selected_ticker != "All":
        filtered_df = filtered_df[filtered_df['ticker'] == selected_ticker]

    # --- Metrics ---
    total_vol = filtered_df['amount_est'].sum()
    # Handle capitalization (Buy vs BUY)
    buy_df = filtered_df[filtered_df['type'].str.contains('Buy', case=False, na=False)]
    sell_df = filtered_df[filtered_df['type'].str.contains('Sell', case=False, na=False)]

    buy_vol = buy_df['amount_est'].sum()
    sell_vol = sell_df['amount_est'].sum()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Visible Volume", f"${total_vol:,.0f}")
    m2.metric("Buy Volume", f"${buy_vol:,.0f}")
    m3.metric("Sell Volume", f"${sell_vol:,.0f}")
    m4.metric("Trades Count", len(filtered_df))

    # --- Visuals ---
    col_charts_1, col_charts_2 = st.columns(2)

    with col_charts_1:
        st.subheader("Money Flow by Sector")
        if 'sector' in filtered_df.columns and not filtered_df.empty:
            sector_grp = filtered_df.groupby('sector')['amount_est'].sum().reset_index()
            fig_pie = px.pie(
                sector_grp,
                values='amount_est',
                names='sector',
                hole=0.4,
                color_discrete_sequence=px.colors.qualitative.Prism
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        else:
            st.info("No sector info available (or no data selected).")

    with col_charts_2:
        st.subheader("Most Active Tickers")
        if not filtered_df.empty:
            ticker_counts = filtered_df['ticker'].value_counts().head(10).reset_index()
            ticker_counts.columns = ['Ticker', 'Trade Count']
            fig_bar = px.bar(
                ticker_counts,
                x='Ticker',
                y='Trade Count',
                text='Trade Count',
                color='Trade Count'
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No trades found matching filters.")

    # --- Data Table ---
    st.subheader("Transaction Log")

    view_cols = ['disclosure_date', 'transaction_date', 'senator', 'ticker', 'type', 'amount_est', 'sector',
                 'asset_description']
    # Safety check for missing columns
    final_cols = [c for c in view_cols if c in filtered_df.columns]

    # Sort by Disclosure Date (Newest first)
    table_df = filtered_df[final_cols].sort_values(by='disclosure_date', ascending=False)

    st.dataframe(
        table_df.style.format({
            "amount_est": "${:,.0f}",
            "disclosure_date": "{:%Y-%m-%d}",
            "transaction_date": "{:%Y-%m-%d}"
        }),
        use_container_width=True,
        height=500
    )


if __name__ == '__main__':
    main()