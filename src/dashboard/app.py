# Minimal Streamlit dashboard for logistics data
import os
import time
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
import streamlit as st
from dotenv import load_dotenv

# Load .env for local development
load_dotenv()

# Configuration (read from env)
DB_HOST = os.getenv('POSTGRES_HOST', 'localhost')
DB_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
DB_NAME = os.getenv('POSTGRES_DB', 'logistics_reports')
DB_USER = os.getenv('POSTGRES_USER', 'admin')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'password123')

st.set_page_config(page_title='Logistics Dashboard', layout='wide')

st.title('Logistics — Live Alerts Dashboard')
st.markdown('Stream of truck telemetry and weather risk alerts. Data source: Postgres `weather_alerts` table.')


@st.cache_resource
def get_db_conn():
    """Return a new DB connection.

    Use st.cache_resource (not st.cache_data) because psycopg2 connection objects are not
    pickle-serializable. This keeps a single connection per session safely cached.
    """
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        cursor_factory=RealDictCursor,
    )
    # Use autocommit to avoid needing to manage transactions here
    conn.autocommit = True
    return conn


@st.cache_data(ttl=10)
def load_recent_alerts(limit: int = 200) -> pd.DataFrame:
    """Load recent alerts from the DB into a DataFrame."""
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, truck_id, lat, lon, weather_main, wind_speed, risk_level, event_timestamp, processed_at
                FROM weather_alerts
                WHERE lat IS NOT NULL AND lon IS NOT NULL
                ORDER BY event_timestamp DESC
                LIMIT %s
                """,
                (limit,)
            )
            rows = cur.fetchall()
        # don't close the cached connection; leave it managed by Streamlit
        if not rows:
            return pd.DataFrame(columns=['id','truck_id','lat','lon','weather_main','wind_speed','risk_level','event_timestamp','processed_at'])
        df = pd.DataFrame(rows)
        # Ensure numeric lat/lon
        df['lat'] = pd.to_numeric(df['lat'], errors='coerce')
        df['lon'] = pd.to_numeric(df['lon'], errors='coerce')
        # Parse timestamps to datetime (ensure timezone-naive UTC)
        df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], utc=True)
        df['processed_at'] = pd.to_datetime(df['processed_at'], utc=True)
        # Fill missing numeric values
        if 'wind_speed' in df.columns:
            df['wind_speed'] = pd.to_numeric(df['wind_speed'], errors='coerce').fillna(0)
        else:
            df['wind_speed'] = 0
        return df
    except Exception as e:
        st.error(f"Error loading alerts from DB: {e}")
        return pd.DataFrame()


def show_overview(df: pd.DataFrame):
    col1, col2, col3, col4 = st.columns(4)
    total = len(df)
    unique_trucks = df['truck_id'].nunique() if not df.empty else 0
    high_risk = len(df[df['risk_level'] == 'HIGH_RISK']) if not df.empty else 0
    low_risk = len(df[df['risk_level'] == 'LOW_RISK']) if not df.empty else 0

    col1.metric('Total alerts (loaded)', total)
    col2.metric('Unique trucks', unique_trucks)
    col3.metric('High risk alerts', high_risk)
    col4.metric('Low risk alerts', low_risk)


def show_map(df: pd.DataFrame, trails_df: pd.DataFrame | None = None, show_trails: bool = False):
    st.subheader('Map — latest truck positions')
    if df.empty:
        st.info('No location data to show on map.')
        return
    # Use the most recent row per truck for markers
    recent_by_truck = df.sort_values('event_timestamp', ascending=False).groupby('truck_id').first().reset_index()
    map_df = recent_by_truck[['lat', 'lon', 'truck_id', 'risk_level', 'wind_speed', 'event_timestamp']].dropna()

    # Prepare colors by risk level
    color_map = {
        'HIGH_RISK': [220, 20, 60],   # crimson
        'LOW_RISK': [34, 139, 34],    # forestgreen
        'UNKNOWN': [128, 128, 128]
    }
    map_df['color'] = map_df['risk_level'].map(lambda r: color_map.get(r, [128, 128, 128]))
    # Visual improvement: smaller base radius and scale by wind; clamp radii for readability
    map_df['radius'] = (map_df['wind_speed'].fillna(0).astype(float) + 0.5) * 500
    map_df['radius'] = map_df['radius'].clip(lower=100, upper=6000)

    # Center view
    center_lat = float(map_df['lat'].mean()) if not map_df['lat'].isna().all() else 0
    center_lon = float(map_df['lon'].mean()) if not map_df['lon'].isna().all() else 0

    try:
        import pydeck as pdk

        layers = []
        # Path layer for trails (if requested)
        if show_trails and trails_df is not None and not trails_df.empty:
            path_rows = []
            # Group by truck and build ordered path of [lon, lat]
            for truck_id, g in trails_df.sort_values('event_timestamp').groupby('truck_id'):
                pts = g.dropna(subset=['lat','lon'])[['lon','lat']].values.tolist()
                if len(pts) < 2:
                    continue
                # color: use most recent risk for that truck if available
                recent_risk = g.sort_values('event_timestamp', ascending=False).iloc[0].get('risk_level')
                color = color_map.get(recent_risk, [128,128,128])
                # Add alpha for trail fading effect by duplicating intermediate points with lower alpha is not supported directly;
                # instead set a solid color and thinner width for trails to avoid clutter
                path_rows.append({'truck_id': truck_id, 'path': pts, 'color': color, 'width': 4})
            if path_rows:
                path_layer = pdk.Layer(
                    "PathLayer",
                    data=path_rows,
                    get_path='path',
                    get_width='width',
                    width_min_pixels=2,
                    get_color='color',
                    cap_style='round',
                    joint_style='round',
                    pickable=False,
                )
                layers.append(path_layer)

        # Scatter layer for latest positions — improved visuals: stroke and pickable popups
        scatter = pdk.Layer(
            "ScatterplotLayer",
            data=map_df,
            get_position='[lon, lat]',
            get_radius='radius',
            radius_min_pixels=6,
            radius_max_pixels=20,
            get_fill_color='color',
            get_line_color='[0,0,0]',
            line_width_min_pixels=1,
            pickable=True,
            auto_highlight=True,
        )
        layers.append(scatter)

        tooltip = {
            "html": "<b>Truck:</b> {truck_id} <br/><b>Risk:</b> {risk_level} <br/><b>Wind:</b> {wind_speed} <br/><b>Time:</b> {event_timestamp}",
            "style": {"color": "white"}
        }
        view = pdk.ViewState(latitude=center_lat, longitude=center_lon, zoom=11, bearing=0, pitch=0)
        deck = pdk.Deck(layers=layers, initial_view_state=view, tooltip=tooltip)
        st.pydeck_chart(deck)
    except Exception as e:
        # Fallback to st.map if pydeck isn't available
        st.warning(f"Map visualization reduced: {e}")
        st.map(map_df.rename(columns={'lat':'lat','lon':'lon'}))

    # Show a small table for context
    st.dataframe(map_df[['truck_id','risk_level','wind_speed','event_timestamp']].set_index('truck_id'))


def show_charts(df: pd.DataFrame):
    st.subheader('Risk distribution')
    if df.empty:
        st.info('No data for charts')
        return
    risk_counts = df['risk_level'].value_counts().rename_axis('risk_level').reset_index(name='count')
    st.bar_chart(risk_counts.set_index('risk_level'))

    st.subheader('Alerts by truck (top 10)')
    trucks = df['truck_id'].value_counts().nlargest(10)
    st.bar_chart(trucks)


def main():
    st.sidebar.header('Controls')
    limit = st.sidebar.slider('Rows to load', min_value=50, max_value=2000, value=500, step=50)
    refresh = st.sidebar.button('Refresh now')

    # Time-range control
    st.sidebar.subheader('Time range')
    time_options = {
        'Last 5 minutes': 5,
        'Last 15 minutes': 15,
        'Last 30 minutes': 30,
        'Last 1 hour': 60,
        'Last 6 hours': 360,
        'Last 24 hours': 1440,
        'All': None,
    }
    selected_range = st.sidebar.selectbox('Show alerts from', list(time_options.keys()), index=1)

    # Custom time range option
    use_custom = st.sidebar.checkbox('Use custom start/end', value=False)
    start_dt = None
    end_dt = None
    if use_custom:
        # Default to last 30 minutes
        now = pd.Timestamp.now(tz='UTC')
        default_start = (now - pd.Timedelta(minutes=30)).to_pydatetime()
        default_end = now.to_pydatetime()
        start_dt = st.sidebar.datetime_input('Start (UTC)', value=default_start)
        end_dt = st.sidebar.datetime_input('End (UTC)', value=default_end)
        # Convert to timezone-aware UTC
        try:
            start_dt = pd.Timestamp(start_dt)
            if start_dt.tzinfo is None:
                start_dt = start_dt.tz_localize('UTC')
            else:
                start_dt = start_dt.tz_convert('UTC')

            end_dt = pd.Timestamp(end_dt)
            if end_dt.tzinfo is None:
                end_dt = end_dt.tz_localize('UTC')
            else:
                end_dt = end_dt.tz_convert('UTC')
        except Exception:
            # Fallback
            start_dt = pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=30)
            end_dt = pd.Timestamp.now(tz='UTC')

    # Auto-refresh controls
    st.sidebar.subheader('Auto-refresh')
    auto_refresh = st.sidebar.checkbox('Enable auto-refresh', value=False)
    refresh_interval = st.sidebar.slider('Interval (sec)', min_value=5, max_value=300, value=30, step=5)

    # Trail controls
    st.sidebar.subheader('Trails')
    show_trails = st.sidebar.checkbox('Show trails', value=False)
    trail_minutes = st.sidebar.slider('Trail window (minutes)', min_value=1, max_value=1440, value=30, step=1)

    # Try to load data
    df = load_recent_alerts(limit)

    # Determine filtered_df based on presets or custom
    if not df.empty:
        if use_custom and start_dt is not None and end_dt is not None:
            if end_dt < start_dt:
                st.sidebar.error('End must be after start')
                filtered_df = df
            else:
                filtered_df = df[(df['event_timestamp'] >= start_dt) & (df['event_timestamp'] <= end_dt)]
        elif time_options[selected_range] is not None:
            cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=time_options[selected_range])
            filtered_df = df[df['event_timestamp'] >= cutoff]
        else:
            filtered_df = df
    else:
        filtered_df = df

    # Compute trails_df (intersection of trail window and selected range)
    trails_df = pd.DataFrame()
    if show_trails and not df.empty:
        now = pd.Timestamp.now(tz='UTC')
        trail_cutoff = now - pd.Timedelta(minutes=trail_minutes)
        # Start with overall trail window
        t_start = trail_cutoff
        t_end = now
        # If custom is used, intersect
        if use_custom and start_dt is not None and end_dt is not None:
            t_start = max(t_start, start_dt)
            t_end = min(t_end, end_dt)
        # If a preset is used, intersect with that as well
        if not use_custom and time_options[selected_range] is not None:
            preset_cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(minutes=time_options[selected_range])
            t_start = max(t_start, preset_cutoff)
        # Filter
        trails_df = df[(df['event_timestamp'] >= t_start) & (df['event_timestamp'] <= t_end)]

    if filtered_df.empty:
        st.warning('No alerts loaded for the selected time range — is the Postgres database running and reachable?')

    show_overview(filtered_df)

    # Layout map + charts
    with st.expander('Map and charts', expanded=True):
        show_map(filtered_df, trails_df=trails_df, show_trails=show_trails)
        show_charts(filtered_df)

    st.subheader('Recent alerts')
    if not filtered_df.empty:
        st.dataframe(filtered_df[['id','truck_id','event_timestamp','weather_main','wind_speed','risk_level']].sort_values('event_timestamp', ascending=False).head(200))

    st.sidebar.markdown('Last updated: ' + time.strftime('%Y-%m-%d %H:%M:%S'))

    # Force a cache refresh if requested
    if refresh:
        load_recent_alerts.clear()
        st.experimental_rerun()

    # Auto-refresh behavior: clear cache and rerun after interval
    if auto_refresh:
        # Sleep briefly (blocks the session) and then rerun to refresh UI
        try:
            time.sleep(refresh_interval)
            load_recent_alerts.clear()
            st.experimental_rerun()
        except Exception:
            pass


if __name__ == '__main__':
    main()
