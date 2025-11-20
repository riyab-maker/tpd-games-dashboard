# ⚠️ All data used in this dashboard must be preprocessed using master_processor.py before deployment.
# This dashboard only handles visualization of preprocessed data to stay within Render's 512MB memory limit.

import os
import json
import pandas as pd
import streamlit as st
from datetime import datetime
from typing import List, Tuple
import re

def _get_db_connection():
    """Return a DB connection using previously used credentials.

    Tries Streamlit secrets first (st.secrets["db"]) then environment variables.
    Supports MySQL/MariaDB via pymysql. Returns None if not available.
    """
    # Prefer st.secrets if available
    try:
        secrets_db = st.secrets.get("db", None) if hasattr(st, "secrets") else None
    except Exception:
        secrets_db = None

    config = None
    if isinstance(secrets_db, dict) and secrets_db:
        config = {
            "host": secrets_db.get("host"),
            "port": int(secrets_db.get("port", 3306)),
            "user": secrets_db.get("user") or secrets_db.get("username"),
            "password": secrets_db.get("password"),
            "database": secrets_db.get("database") or secrets_db.get("db")
        }
    else:
        # Fallback to environment variables
        config = {
            "host": os.getenv("DB_HOST") or os.getenv("MYSQL_HOST"),
            "port": int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or 3306),
            "user": os.getenv("DB_USER") or os.getenv("MYSQL_USER") or os.getenv("DB_USERNAME"),
            "password": os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD"),
            "database": os.getenv("DB_NAME") or os.getenv("MYSQL_DATABASE") or os.getenv("MYSQL_DB")
        }

    # Validate minimal config
    if not config or not config.get("host") or not config.get("user") or not config.get("database"):
        return None

    try:
        import pymysql  # type: ignore
        conn = pymysql.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
    password=config["password"],
            database=config["database"],
            cursorclass=pymysql.cursors.DictCursor,
            read_timeout=30,
            write_timeout=30,
            charset="utf8mb4"
        )
        return conn
    except Exception as e:
        st.info(f"Skipping live poll data refresh (DB connection unavailable): {e}")
        return None

def _parse_poll_fields(custom_dimension_1: str) -> Tuple[str, str]:
    """Best-effort parsing of question and option from custom_dimension_1.

    Returns (question, option). If parsing fails, returns ("Parent Poll", cleaned_string).
    """
    if not isinstance(custom_dimension_1, str) or not custom_dimension_1.strip():
        return ("Parent Poll", "Unknown")

    val = custom_dimension_1.strip()

    # Try JSON payload first
    try:
        obj = json.loads(val)
        if isinstance(obj, dict):
            q = obj.get("question") or obj.get("q") or obj.get("poll_question")
            o = obj.get("option") or obj.get("answer") or obj.get("value")
            if q and o:
                return (str(q), str(o))
    except Exception:
        pass

    # Common patterns: "poll:Question|Option", "poll_Question_Option", "Question: Option"
    patterns = [
        re.compile(r"poll[:_\-\s]+([^|:_\-]+)[|:_\-\s]+(.+)$", re.IGNORECASE),
        re.compile(r"([^|:]+)[|:]+\s*(.+)$"),
    ]
    for pat in patterns:
        m = pat.search(val)
        if m:
            q = m.group(1).strip()
            o = m.group(2).strip()
            if q and o:
                return (q, o)

    # Fallback: single question, option as entire string after 'poll'
    # Remove leading 'poll' tokens
    val_clean = re.sub(r"^poll[:_\-\s]*", "", val, flags=re.IGNORECASE).strip()
    if not val_clean:
        val_clean = val
    return ("Parent Poll", val_clean)

def try_refresh_poll_responses_data() -> None:
    """Attempt to refresh poll_responses_data.csv from the live DB using provided SQL.

    Silently no-ops if DB is not available. Keeps existing structure/visuals intact.
    """
    conn = _get_db_connection()
    if conn is None:
        return
    
    SQL = (
        "select * "
        "\n\n\n\nfrom `matomo_log_link_visit_action` "
        "\n\tinner join `matomo_log_action` on `matomo_log_link_visit_action`.`idaction_name` = `matomo_log_action`.`idaction` "
        "\n\tinner join `hybrid_games_links` on `hybrid_games_links`.`activity_id` = `matomo_log_link_visit_action`.`custom_dimension_2` "
        "\n\tinner join `hybrid_games` on `hybrid_games`.`id` = `hybrid_games_links`.`game_id` "
        "\n\nwhere `matomo_log_action`.`name` like \"%_completed%\" "
        "\n\tand `matomo_log_link_visit_action`.`custom_dimension_1` is not null "
        "\n\tand `matomo_log_link_visit_action`.`custom_dimension_1` like \"%poll%\" "
        "\n\tand `matomo_log_link_visit_action`.`server_time` > '2025-07-01' "
        "\n\tand `hybrid_games_links`.`activity_id` is not null;"
    )

    try:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall() or []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    if not rows:
        return
    
    # Build poll response aggregates with expected columns
    records = []
    for r in rows:
        cd1 = r.get("custom_dimension_1")
        q, o = _parse_poll_fields(cd1)
        # Try different possible game name columns
        game_name = (
            r.get("game_name")
            or r.get("name")
            or r.get("title")
            or r.get("hybrid_games.name")
        )
        # Avoid collision with matomo_log_action.name; prefer hybrid_games column
        if isinstance(game_name, str) and r.get("name") == game_name:
            pass
        elif not isinstance(game_name, str):
            # If not found, and there is a separate games table alias, leave as Unknown
            game_name = "Unknown"
        records.append({"question": q, "option": o, "game_name": game_name})

    df = pd.DataFrame.from_records(records)
    if df.empty:
        return

    # Aggregate counts
    agg_cols = ["question", "option", "game_name"] if "game_name" in df.columns else ["question", "option"]
    agg = df.groupby(agg_cols).size().reset_index(name="count")

    # Ensure stable column order
    final_cols = ["question", "option", "count"]
    if "game_name" in agg.columns:
        final_cols.insert(2, "game_name")
    result_df = agg[final_cols]

    # Write to CSV
    os.makedirs(DATA_DIR, exist_ok=True)
    output_path = os.path.join(DATA_DIR, "poll_responses_data.csv")
    try:
        result_df.to_csv(output_path, index=False)
        st.info("Parent Poll Responses data refreshed from database.")
    except Exception as e:
        st.warning(f"Could not write refreshed poll data: {e}")

# Use preprocess_data.py directly instead of processed CSV files
DATA_DIR = "data"
REQUIRED_FILES = [
    "summary_data.csv",
    "time_series_data.csv",
    "repeatability_data.csv",
    "score_distribution_data.csv",
    "game_conversion_numbers.csv",
    "poll_responses_data.csv"
]

def check_processed_data():
    """Check if all required processed data files exist"""
    # Check if DATA_DIR exists
    if not os.path.exists(DATA_DIR):
        st.error(f"ERROR: Data directory does not exist: {DATA_DIR}")
        st.error("Please ensure data folder is uploaded to GitHub.")
        st.stop()
    
    missing_files = []
    for file in REQUIRED_FILES:
        file_path = os.path.join(DATA_DIR, file)
        if not os.path.exists(file_path):
            missing_files.append(file)
    
    if missing_files:
        st.error(f"Missing processed data files: {', '.join(missing_files)}")
        st.error("Please ensure all data files are uploaded to GitHub before deployment.")
        st.error("This dashboard only works with preprocessed data files.")
        st.stop()
    
    return True

def load_processed_data():
    """Load all data files from data/ directory"""
    try:
        # Load game-specific conversion numbers (final numbers for individual games)
        game_conversion_df = pd.read_csv(os.path.join(DATA_DIR, "game_conversion_numbers.csv"))
        # Ensure numeric columns are properly typed
        numeric_cols = ['started_users', 'completed_users', 'started_visits', 'completed_visits', 'started_instances', 'completed_instances']
        for col in numeric_cols:
            if col in game_conversion_df.columns:
                game_conversion_df[col] = pd.to_numeric(game_conversion_df[col], errors='coerce').fillna(0).astype(int)
        
        # Load processed data for date filtering (aggregated by date, game, event)
        processed_data_path = os.path.join(DATA_DIR, "processed_data.csv")
        if os.path.exists(processed_data_path):
            processed_data_df = pd.read_csv(processed_data_path)
            # Check if it's the new aggregated format (has 'date' column) or old format (has 'server_time')
            if 'date' in processed_data_df.columns:
                # New aggregated format: date, game_name, event, instances, visits, users
                processed_data_df['date'] = pd.to_datetime(processed_data_df['date']).dt.date
                # Ensure numeric columns are properly typed
                for col in ['instances', 'visits', 'users']:
                    if col in processed_data_df.columns:
                        processed_data_df[col] = pd.to_numeric(processed_data_df[col], errors='coerce').fillna(0).astype(int)
            elif 'server_time' in processed_data_df.columns:
                # Old format: convert server_time to date
                processed_data_df['server_time'] = pd.to_datetime(processed_data_df['server_time'])
                processed_data_df['date'] = processed_data_df['server_time'].dt.date
            else:
                # Unknown format - create empty DataFrame
                processed_data_df = pd.DataFrame(columns=['date', 'game_name', 'event', 'instances', 'visits', 'users'])
        else:
            # Create empty DataFrame with expected columns if file doesn't exist
            processed_data_df = pd.DataFrame(columns=['date', 'game_name', 'event', 'instances', 'visits', 'users'])
            st.info("ℹ️ Note: Date filtering is disabled because processed_data.csv is not available. Using summary data without date filtering.")
        
        # Load summary data for conversion funnels
        summary_df = pd.read_csv(os.path.join(DATA_DIR, "summary_data.csv"))
        # Ensure numeric columns are properly typed
        if 'Users' in summary_df.columns:
            summary_df['Users'] = pd.to_numeric(summary_df['Users'], errors='coerce').fillna(0).astype(int)
        if 'Visits' in summary_df.columns:
            summary_df['Visits'] = pd.to_numeric(summary_df['Visits'], errors='coerce').fillna(0).astype(int)
        if 'Instances' in summary_df.columns:
            summary_df['Instances'] = pd.to_numeric(summary_df['Instances'], errors='coerce').fillna(0).astype(int)
        
        # Load time series data
        time_series_df = pd.read_csv(os.path.join(DATA_DIR, "time_series_data.csv"))
        
        # Load repeatability data
        repeatability_df = pd.read_csv(os.path.join(DATA_DIR, "repeatability_data.csv"))
        
        # Load score distribution data
        score_distribution_df = pd.read_csv(os.path.join(DATA_DIR, "score_distribution_data.csv"))
        
        # Load poll responses data
        poll_responses_df = pd.read_csv(os.path.join(DATA_DIR, "poll_responses_data.csv"))

        # Load per-question correctness data (optional)
        qpath = os.path.join(DATA_DIR, "question_correctness_data.csv")
        if os.path.exists(qpath):
            question_correctness_df = pd.read_csv(qpath)
        else:
            question_correctness_df = pd.DataFrame()
        
        # Create metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'data_source': 'data',
            'version': '2.0'
        }
        
        return (summary_df, game_conversion_df, time_series_df, 
                repeatability_df, score_distribution_df, poll_responses_df, question_correctness_df, metadata, processed_data_df)
    
    except Exception as e:
        st.error(f"❌ Error loading processed data: {str(e)}")
        st.error("Please ensure all data files are properly generated by running preprocess_data.py")
        st.stop()

def _render_altair_chart(chart, use_container_width=True):
    """Helper function to render Altair chart with version-compatible width parameter"""
    import streamlit as st
    # Always use use_container_width for compatibility with all Streamlit versions
    # (Newer versions show deprecation warnings but it still works)
    return st.altair_chart(chart, use_container_width=use_container_width)

def render_modern_dashboard(conversion_df: pd.DataFrame, df_filtered: pd.DataFrame) -> None:
    """Render a modern, professional dashboard with multiple chart types"""
    import altair as alt
    
    # Add separate conversion funnels
    st.markdown("### 🔄 Conversion Funnels")
    
    # Display date range for conversion funnel - safe access
    try:
        if 'Event' in conversion_df.columns:
            started_row = conversion_df[conversion_df['Event'] == 'Started']
            completed_row = conversion_df[conversion_df['Event'] == 'Completed']
            if not started_row.empty and len(started_row) > 0 and 'Users' in started_row.columns:
                started_count = int(pd.to_numeric(started_row['Users'], errors='coerce').fillna(0).iloc[0])
            else:
                started_count = 0
            if not completed_row.empty and len(completed_row) > 0 and 'Users' in completed_row.columns:
                completed_count = int(pd.to_numeric(completed_row['Users'], errors='coerce').fillna(0).iloc[0])
            else:
                completed_count = 0
        else:
            if 'event' in conversion_df.columns:
                started_count = conversion_df[conversion_df['event'] == 'Started']['idvisitor_converted'].nunique()
                completed_count = conversion_df[conversion_df['event'] == 'Completed']['idvisitor_converted'].nunique()
            else:
                started_count = 0
                completed_count = 0
    except (IndexError, KeyError, ValueError, TypeError):
        started_count = 0
        completed_count = 0

    # Get data for each funnel from conversion data
    # Funnel stages order: started, introduction, mid_introduction, validation, questions, parent_poll, rewards, completed
    funnel_stages = ['started', 'introduction', 'mid_introduction', 'validation', 'questions', 'parent_poll', 'rewards', 'completed']
    stage_labels = {
        'started': 'Started',
        'introduction': 'Introduction',
        'mid_introduction': 'Mid Introduction',
        'parent_poll': 'Parent Poll',
        'validation': 'Validation',
        'rewards': 'Rewards',
        'questions': 'Questions',
        'completed': 'Completed'
    }
    
    if 'Event' in conversion_df.columns:
        # Summary data format (total data)
        # Extract data for all funnel stages
        funnel_data = {}
        for stage in funnel_stages:
            stage_row = conversion_df[conversion_df['Event'] == stage]
            try:
                if not stage_row.empty and len(stage_row) > 0 and 'Users' in stage_row.columns:
                    funnel_data[f'{stage}_users'] = int(pd.to_numeric(stage_row['Users'], errors='coerce').fillna(0).iloc[0])
                    funnel_data[f'{stage}_visits'] = int(pd.to_numeric(stage_row['Visits'], errors='coerce').fillna(0).iloc[0])
                    funnel_data[f'{stage}_instances'] = int(pd.to_numeric(stage_row['Instances'], errors='coerce').fillna(0).iloc[0])
                else:
                    funnel_data[f'{stage}_users'] = 0
                    funnel_data[f'{stage}_visits'] = 0
                    funnel_data[f'{stage}_instances'] = 0
            except (IndexError, KeyError, ValueError, TypeError):
                funnel_data[f'{stage}_users'] = 0
                funnel_data[f'{stage}_visits'] = 0
                funnel_data[f'{stage}_instances'] = 0
    else:
        # Filtered data format - calculate from raw data
        for stage in funnel_stages:
            stage_data = conversion_df[conversion_df['event'] == stage]
            funnel_data[f'{stage}_users'] = stage_data['idvisitor_converted'].nunique()
            funnel_data[f'{stage}_visits'] = stage_data['idvisit'].nunique()
            funnel_data[f'{stage}_instances'] = len(stage_data)
    
    
    # Create three separate funnels with all stages
    # Order: Instances first, then Visits, then Users
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("#### ⚡ Instances Funnel")
        instances_data = pd.DataFrame([
            {'Stage': stage_labels[stage], 'Count': funnel_data.get(f'{stage}_instances', 0), 'Order': idx}
            for idx, stage in enumerate(funnel_stages)
        ])
        
        instances_chart = alt.Chart(instances_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#F5A623'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=600  # Increased height for 8 stages
        )
        
        # Add labels with complete numbers - positioned at the start of bars
        instances_labels = alt.Chart(instances_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        instances_funnel = alt.layer(instances_chart, instances_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        _render_altair_chart(instances_funnel, use_container_width=True)
    
    with col2:
        st.markdown("#### 🔄 Visits Funnel")
        visits_data = pd.DataFrame([
            {'Stage': stage_labels[stage], 'Count': funnel_data.get(f'{stage}_visits', 0), 'Order': idx}
            for idx, stage in enumerate(funnel_stages)
        ])
        
        visits_chart = alt.Chart(visits_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#7ED321'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=600  # Increased height for 8 stages
        )
        
        # Add labels with complete numbers - positioned at the start of bars
        visits_labels = alt.Chart(visits_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        visits_funnel = alt.layer(visits_chart, visits_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        _render_altair_chart(visits_funnel, use_container_width=True)
    
    with col3:
        st.markdown("#### 👥 Users Funnel")
        users_data = pd.DataFrame([
            {'Stage': stage_labels[stage], 'Count': funnel_data.get(f'{stage}_users', 0), 'Order': idx}
            for idx, stage in enumerate(funnel_stages)
        ])
        
        users_chart = alt.Chart(users_data).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#4A90E2'
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending'), title=''),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
            width=300,
            height=600  # Increased height for 8 stages
        )
        
        # Add labels with complete numbers - positioned at the start of bars
        users_labels = alt.Chart(users_data).mark_text(
            align='left',
            baseline='middle',
            color='white',
            fontSize=22,
            fontWeight='bold',
            dx=10
        ).encode(
            x=alt.value(10),  # Fixed position at the start
            y=alt.Y('Stage:N', sort=alt.SortField(field='Order', order='ascending')),
            text=alt.Text('Count:Q', format='.0f')
        )
        
        users_funnel = alt.layer(users_chart, users_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
        _render_altair_chart(users_funnel, use_container_width=True)
    
    # Add conversion analysis
    st.markdown("### 📊 Conversion Analysis")
    
    # Extract values from funnel_data dictionary
    started_users = funnel_data.get('started_users', 0)
    completed_users = funnel_data.get('completed_users', 0)
    started_visits = funnel_data.get('started_visits', 0)
    completed_visits = funnel_data.get('completed_visits', 0)
    started_instances = funnel_data.get('started_instances', 0)
    completed_instances = funnel_data.get('completed_instances', 0)
    
    analysis_col1, analysis_col2, analysis_col3 = st.columns(3)
    
    with analysis_col1:
        user_conversion = (completed_users / started_users * 100) if started_users > 0 else 0
        st.metric(
            label="👥 User Conversion",
            value=f"{user_conversion:.1f}%",
            help=f"{completed_users:,} out of {started_users:,} users completed"
        )
    
    with analysis_col2:
        visit_conversion = (completed_visits / started_visits * 100) if started_visits > 0 else 0
        st.metric(
            label="🔄 Visit Conversion",
            value=f"{visit_conversion:.1f}%",
            help=f"{completed_visits:,} out of {started_visits:,} visits completed"
        )
    
    with analysis_col3:
        instance_conversion = (completed_instances / started_instances * 100) if started_instances > 0 else 0
        st.metric(
            label="⚡ Instance Conversion",
            value=f"{instance_conversion:.1f}%",
            help=f"{completed_instances:,} out of {started_instances:,} instances completed"
        )
    
def render_score_distribution_chart(score_distribution_df: pd.DataFrame) -> None:
    """Render score distribution chart"""
    import altair as alt
    
    
    if score_distribution_df.empty:
        st.warning("No score distribution data available.")
        return
    
    # Get unique games for filter
    unique_games = sorted(score_distribution_df['game_name'].unique())
    
    # Add game filter
    st.markdown("**🎮 Game Filter:**")
    selected_games = st.multiselect(
        "Select Games for Score Distribution:",
        options=unique_games,
        default=[unique_games[0]] if unique_games else [],  # Show first game by default
        help="Select one or more games to show score distribution. First game is selected by default."
    )
    
    # Filter data based on selected games
    if selected_games:
        filtered_df = score_distribution_df[score_distribution_df['game_name'].isin(selected_games)]
    else:
        # If no games selected, show first game
        filtered_df = score_distribution_df[score_distribution_df['game_name'] == unique_games[0]] if unique_games else score_distribution_df
    
    if filtered_df.empty:
        st.warning("No data available for the selected games.")
        return
    
    # Create the score distribution chart
    st.markdown("### 📊 Score Distribution")
    if len(selected_games) == 1:
        st.markdown(f"This chart shows how many users achieved each total score for **{selected_games[0]}**.")
    else:
        st.markdown(f"This chart shows how many users achieved each total score for the selected games ({len(selected_games)} games).")
    
    # Add score distribution chart
    st.markdown("#### 🎯 Score Distribution")
    
    # Create combined data by summing user counts across all games for each score
    combined_df = filtered_df.groupby('total_score')['user_count'].sum().reset_index()
    combined_df.columns = ['total_score', 'total_users']
    
    if not combined_df.empty:
        # Create combined bar chart
        combined_bars = alt.Chart(combined_df).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#FF6B6B'
        ).encode(
            x=alt.X('total_score:O', 
                    title='Total Score', 
                    axis=alt.Axis(
                        labelAngle=0,
                        labelFontSize=14,
                        titleFontSize=16,
                        labelLimit=100
                    )),
            y=alt.Y('total_users:Q', 
                    title='Total Number of Users', 
                    axis=alt.Axis(format='~s')),
            tooltip=['total_score:O', 'total_users:Q']
        ).properties(
            width=900,
            height=400,
            title='Score Distribution'
        )
        
        # Add data labels for combined chart
        combined_labels = alt.Chart(combined_df).mark_text(
            align='center',
            baseline='bottom',
            color='#2E8B57',
            fontSize=12,
            fontWeight='bold',
            dy=-5
        ).encode(
            x=alt.X('total_score:O'),
            y=alt.Y('total_users:Q'),
            text=alt.Text('total_users:Q', format='.0f')
        )
        
        # Combine bars and labels for combined chart
        combined_chart = (combined_bars + combined_labels).configure_axis(
            labelFontSize=16,
            titleFontSize=18,
            grid=True
        ).configure_title(
            fontSize=24,
            fontWeight='bold'
        )
        
        _render_altair_chart(combined_chart, use_container_width=True)
    
    # Add summary statistics after the chart
    st.markdown("#### 📈 Summary Statistics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_users = filtered_df['user_count'].sum()
        st.metric(
            label="👥 Total Users",
            value=f"{total_users:,}",
            help="Total number of users across all games and scores"
        )
    
    with col2:
        avg_score = (filtered_df['total_score'] * filtered_df['user_count']).sum() / filtered_df['user_count'].sum()
        st.metric(
            label="🎯 Average Score",
            value=f"{avg_score:.1f}",
            help="Weighted average score across all users"
        )
    
    with col3:
        max_score = filtered_df['total_score'].max()
        st.metric(
            label="🏆 Highest Score",
            value=f"{max_score}",
            help="Maximum total score achieved"
        )
    
    with col4:
        unique_games_count = filtered_df['game_name'].nunique()
        st.metric(
            label="🎮 Games Analyzed",
            value=f"{unique_games_count}",
            help="Number of games included in the analysis"
        )


def render_repeatability_analysis(repeatability_df: pd.DataFrame) -> None:
    """Render game repeatability analysis based on SQL query logic:
    
    SQL Query Logic:
    1. JOIN hybrid_games, hybrid_games_links, hybrid_game_completions, hybrid_profiles, hybrid_users
    2. Group by hybrid_profile_id
    3. Count distinct non-null values of game_name for each hybrid_profile_id
    4. Group by the count of distinct non-null game_name
    5. Calculate CountDistinct_hybrid_profile_id for each distinct count value
    
    Visualization:
    X-axis → number of distinct games played (repeat count)
    Y-axis → number of unique users (hybrid_profile_id) corresponding to each repeat count
    """
    import altair as alt
    
    if repeatability_df.empty:
        st.warning("No repeatability data available.")
        return
    
    st.markdown("### 🎮 Game Repeatability Analysis")
    
    # Create the repeatability chart with explicit axis configuration
    chart = alt.Chart(repeatability_df).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            color='#50C878'
        ).encode(
        x=alt.X('games_played:O', 
                title='No of games played', 
                axis=alt.Axis(labelAngle=0, titleFontSize=24, labelFontSize=22)),
        y=alt.Y('user_count:Q', 
                title='Number of Users', 
                axis=alt.Axis(format='~s', titleFontSize=24, labelFontSize=22)),
            tooltip=['games_played:O', 'user_count:Q']
        ).properties(
            width=800,
            height=400,
        title='User Distribution by Number of Distinct Games Completed'
        )
        
    # Add text labels
    text = alt.Chart(repeatability_df).mark_text(
            align='center',
            baseline='bottom',
            color='#2E8B57',
            fontSize=20,
            fontWeight='bold',
            dy=-10
        ).encode(
            x=alt.X('games_played:O'),
            y=alt.Y('user_count:Q'),
            text=alt.Text('user_count:Q', format='.0f')
        )
        
    # Combine chart and text
    repeatability_chart = (chart + text).configure_axis(
            grid=True
        ).configure_title(
            fontSize=28,
            fontWeight='bold'
        )
    
    _render_altair_chart(repeatability_chart, use_container_width=True)
    
    # Add summary statistics based on SQL query logic
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_users = repeatability_df['user_count'].sum()
        st.metric(
            label="👥 Total Unique Users",
            value=f"{total_users:,}",
            help="Total number of unique hybrid_profile_id who completed at least one distinct game"
        )
    
    with col2:
        # Calculate weighted average of distinct games per user
        weighted_sum = (repeatability_df['games_played'] * repeatability_df['user_count']).sum()
        total_users = repeatability_df['user_count'].sum()
        avg_distinct_games_per_user = weighted_sum / total_users if total_users > 0 else 0
        st.metric(
            label="🎯 Avg Distinct Games per User",
            value=f"{avg_distinct_games_per_user:.1f}",
            help="Average number of distinct games completed per hybrid_profile_id"
        )
    
    with col3:
        max_distinct_games = repeatability_df['games_played'].max()
        st.metric(
            label="🏆 Max Distinct Games",
            value=f"{max_distinct_games}",
            help="Maximum number of distinct games completed by a single hybrid_profile_id"
        )

def recalculate_time_series_for_games(df_main: pd.DataFrame, time_period: str) -> pd.DataFrame:
    """Recalculate time series data for selected games"""
    if df_main.empty:
        return pd.DataFrame()
    
    # Convert date to datetime (use the correct date column)
    df_main['datetime'] = pd.to_datetime(df_main['date'])
    
    # Filter to July 2nd, 2025 onwards
    july_2_2025 = pd.to_datetime('2025-07-02')
    df_main = df_main[df_main['datetime'] >= july_2_2025]
    
    time_series_data = []
    
    if time_period == "Day":
        # Day-level data (last 2 weeks from available data)
        cutoff_date = df_main['datetime'].max() - pd.Timedelta(days=14)
        df_filtered = df_main[df_main['datetime'] >= cutoff_date].copy()
        df_filtered['time_group'] = df_filtered['datetime'].dt.date
        
        for time_group in df_filtered['time_group'].unique():
            group_data = df_filtered[df_filtered['time_group'] == time_group]
            
            started_users = group_data[group_data['event'] == 'Started']['idvisitor_converted'].nunique()
            completed_users = group_data[group_data['event'] == 'Completed']['idvisitor_converted'].nunique()
            started_visits = group_data[group_data['event'] == 'Started']['idvisit'].nunique()
            completed_visits = group_data[group_data['event'] == 'Completed']['idvisit'].nunique()
            started_instances = len(group_data[group_data['event'] == 'Started'])
            completed_instances = len(group_data[group_data['event'] == 'Completed'])
            
            time_series_data.append({
                'time_period': str(time_group),
                'period_type': 'Day',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances
            })
    
    elif time_period == "Week":
        # Week-level data (all available data grouped by weeks)
        # Use the earliest date in the data as the reference point
        start_date = df_main['datetime'].min()
        df_filtered = df_main.copy()
        df_filtered['days_since_start'] = (df_filtered['datetime'] - start_date).dt.days
        df_filtered['week_number'] = (df_filtered['days_since_start'] // 7) + 1
        df_filtered['time_group_week'] = 'Week ' + df_filtered['week_number'].astype(str)
        
        for time_group in df_filtered['time_group_week'].unique():
            group_data = df_filtered[df_filtered['time_group_week'] == time_group]
            
            started_users = group_data[group_data['event'] == 'Started']['idvisitor_converted'].nunique()
            completed_users = group_data[group_data['event'] == 'Completed']['idvisitor_converted'].nunique()
            started_visits = group_data[group_data['event'] == 'Started']['idvisit'].nunique()
            completed_visits = group_data[group_data['event'] == 'Completed']['idvisit'].nunique()
            started_instances = len(group_data[group_data['event'] == 'Started'])
            completed_instances = len(group_data[group_data['event'] == 'Completed'])
            
            time_series_data.append({
                'time_period': time_group,
                'period_type': 'Week',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances
            })
    
    elif time_period == "Month":
        # Month-level data (all available data grouped by months)
        df_filtered = df_main.copy()
        df_filtered['time_group_month'] = df_filtered['datetime'].dt.strftime('%B %Y')
        
        for time_group in df_filtered['time_group_month'].unique():
            group_data = df_filtered[df_filtered['time_group_month'] == time_group]
            
            started_users = group_data[group_data['event'] == 'Started']['idvisitor_converted'].nunique()
            completed_users = group_data[group_data['event'] == 'Completed']['idvisitor_converted'].nunique()
            started_visits = group_data[group_data['event'] == 'Started']['idvisit'].nunique()
            completed_visits = group_data[group_data['event'] == 'Completed']['idvisit'].nunique()
            started_instances = len(group_data[group_data['event'] == 'Started'])
            completed_instances = len(group_data[group_data['event'] == 'Completed'])
            
            time_series_data.append({
                'time_period': time_group,
                'period_type': 'Month',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances
            })
    
    elif time_period == "All time":
        # All time data (all available data grouped by months)
        # Use the same logic as Month but with different period_type
        df_filtered = df_main.copy()
        df_filtered['time_group_month'] = df_filtered['datetime'].dt.strftime('%B %Y')
        
        for time_group in df_filtered['time_group_month'].unique():
            group_data = df_filtered[df_filtered['time_group_month'] == time_group]
            
            started_users = group_data[group_data['event'] == 'Started']['idvisitor_converted'].nunique()
            completed_users = group_data[group_data['event'] == 'Completed']['idvisitor_converted'].nunique()
            started_visits = group_data[group_data['event'] == 'Started']['idvisit'].nunique()
            completed_visits = group_data[group_data['event'] == 'Completed']['idvisit'].nunique()
            started_instances = len(group_data[group_data['event'] == 'Started'])
            completed_instances = len(group_data[group_data['event'] == 'Completed'])
            
            time_series_data.append({
                'time_period': time_group,
                'period_type': 'All time',
                'started_users': started_users,
                'completed_users': completed_users,
                'started_visits': started_visits,
                'completed_visits': completed_visits,
                'started_instances': started_instances,
                'completed_instances': completed_instances
            })
        
    return pd.DataFrame(time_series_data)

def render_time_series_analysis(time_series_df: pd.DataFrame, game_conversion_df: pd.DataFrame) -> None:
    """Render time series analysis with separate charts for Instances, Visits, and Users
    Each chart shows Started and Completed bars"""
    import altair as alt
    
    if time_series_df.empty:
        st.warning("No time series data available.")
        return
    
    st.markdown("### 📈 Time-Series Analysis")
    
    # Create columns for filters
    ts_filter_col1, ts_filter_col2 = st.columns(2)
    
    with ts_filter_col1:
        # Time period filter
        time_period = st.selectbox(
            "Select Time Period:",
            options=["Monthly", "Weekly", "Daily"],
            help="Choose the time aggregation for the time-series graphs"
        )
    
    with ts_filter_col2:
        # Game filter for time series
        unique_games_ts = sorted(game_conversion_df['game_name'].unique())
        selected_games_ts = st.multiselect(
            "Select Games:",
            options=unique_games_ts,
            default=[],  # Empty by default - shows all games
            help="Select games to include in time series analysis. Leave empty to show all games."
        )
    
    # Filter by selected time period
    period_type_map = {"Daily": "Day", "Weekly": "Week", "Monthly": "Month"}
    period_filter = period_type_map.get(time_period, time_period)
    filtered_ts_df = time_series_df[time_series_df['period_type'] == period_filter].copy()
    
    # Apply game filtering
    if selected_games_ts and len(selected_games_ts) > 0:
        filtered_ts_df = filtered_ts_df[filtered_ts_df['game_name'].isin(selected_games_ts)]
        # Aggregate across selected games
        aggregated_df = filtered_ts_df.groupby(['period_label', 'metric', 'event']).agg({'count': 'sum'}).reset_index()
        aggregated_df['game_name'] = 'Selected Games'
    else:
        # Use "All Games" data
        filtered_ts_df = filtered_ts_df[filtered_ts_df['game_name'] == 'All Games']
        aggregated_df = filtered_ts_df[['period_label', 'metric', 'event', 'count']].copy()
    
    if aggregated_df.empty:
        st.warning("No data available for the selected time period.")
        return
    
    # Format time period labels for display
    def format_time_label(period_label, period_type):
        if period_type == "Monthly":
            try:
                from datetime import datetime
                date_obj = datetime.strptime(period_label.replace('_', '-') + '-01', '%Y-%m-%d')
                return date_obj.strftime('%B %Y')
            except:
                return str(period_label)
        elif period_type == "Weekly":
            parts = str(period_label).split('_')
            if len(parts) == 2:
                return f"Week {parts[1]}"  # Just show week number without year
            else:
                return str(period_label)
        elif period_type == "Daily":
            try:
                from datetime import datetime
                date_obj = datetime.strptime(period_label, '%Y-%m-%d')
                return date_obj.strftime('%b %d, %Y')
            except:
                return str(period_label)
        return str(period_label)
    
    # Add formatted time labels
    aggregated_df['time_display'] = aggregated_df['period_label'].apply(lambda x: format_time_label(x, time_period))
    
    # Sort by period_label
    if time_period == "Monthly":
        try:
            aggregated_df['sort_date'] = pd.to_datetime(aggregated_df['period_label'].str.replace('_', '-') + '-01')
            aggregated_df = aggregated_df.sort_values('sort_date').drop('sort_date', axis=1)
        except:
            pass
    elif time_period == "Weekly":
        try:
            parts = aggregated_df['period_label'].str.split('_', expand=True)
            aggregated_df['year'] = parts[0].astype(int)
            aggregated_df['week'] = parts[1].astype(int)
            aggregated_df = aggregated_df.sort_values(['year', 'week']).drop(['year', 'week'], axis=1)
        except:
            pass
    elif time_period == "Daily":
        try:
            aggregated_df['sort_date'] = pd.to_datetime(aggregated_df['period_label'])
            aggregated_df = aggregated_df.sort_values('sort_date')
            # Filter to only show latest 14 days
            if len(aggregated_df) > 0:
                latest_date = aggregated_df['sort_date'].max()
                cutoff_date = latest_date - pd.Timedelta(days=13)  # 14 days including latest
                aggregated_df = aggregated_df[aggregated_df['sort_date'] >= cutoff_date]
            aggregated_df = aggregated_df.drop('sort_date', axis=1)
        except:
            pass
    
    # Get time order for x-axis - ensure chronological order
    if time_period == "Monthly":
        # Sort by actual date for chronological order
        try:
            time_order_df = pd.DataFrame({'time_display': aggregated_df['time_display'].unique()})
            time_order_df['sort_key'] = time_order_df['time_display'].apply(
                lambda x: pd.to_datetime(x, format='%B %Y', errors='coerce')
            )
            time_order_df = time_order_df.sort_values('sort_key')
            time_order = time_order_df['time_display'].tolist()
        except:
            time_order = sorted(aggregated_df['time_display'].unique().tolist())
    elif time_period == "Weekly":
        # Sort by year and week for chronological order
        try:
            time_order_df = pd.DataFrame({'time_display': aggregated_df['time_display'].unique()})
            time_order_df['year'] = time_order_df['time_display'].str.extract(r'\((\d{4})\)').astype(int)
            time_order_df['week'] = time_order_df['time_display'].str.extract(r'Week (\d+)').astype(int)
            time_order_df = time_order_df.sort_values(['year', 'week'])
            time_order = time_order_df['time_display'].tolist()
        except:
            time_order = sorted(aggregated_df['time_display'].unique().tolist())
    elif time_period == "Daily":
        # Sort by date for chronological order
        try:
            time_order_df = pd.DataFrame({'time_display': aggregated_df['time_display'].unique()})
            time_order_df['sort_key'] = pd.to_datetime(time_order_df['time_display'], format='%b %d, %Y', errors='coerce')
            time_order_df = time_order_df.sort_values('sort_key')
            time_order = time_order_df['time_display'].tolist()
        except:
            time_order = sorted(aggregated_df['time_display'].unique().tolist())
    else:
        time_order = sorted(aggregated_df['time_display'].unique().tolist())
    
    # Calculate dynamic width based on number of time periods
    # Use consistent calculation to ensure similar spacing per time period across all views
    num_periods = len(time_order)
    # Calculate width to give similar space per time period across all views
    # This ensures consistent bar spacing within groups
    base_width_per_period = 100  # Base width per time period for consistent spacing
    if time_period == "Daily":
        chart_width = max(900, num_periods * base_width_per_period)
    elif time_period == "Weekly":
        chart_width = max(800, num_periods * base_width_per_period)
    else:  # Monthly
        chart_width = max(700, num_periods * base_width_per_period)
    
    # Create two charts - one for Started, one for Completed
    st.markdown("### 📊 Time Series Analysis: Started vs Completed")
    st.markdown("Each chart shows **Instances**, **Visits**, and **Users** side by side for each time period.")
    
    # Define metrics configuration
    metrics_config = [
        {'name': 'Instances', 'color': '#4A90E2', 'label': 'Instances'},
        {'name': 'Visits', 'color': '#50C878', 'label': 'Visits'},
        {'name': 'Users', 'color': '#FFA726', 'label': 'Users'}
    ]
    
    # Create charts for Started and Completed
    event_types = ['Started', 'Completed']
    event_titles = {'Started': '🚀 Started Events', 'Completed': '✅ Completed Events'}
    
    for event_type in event_types:
        st.markdown(f"#### {event_titles[event_type]}")
        
        # Filter data for this event type
        event_data = aggregated_df[aggregated_df['event'] == event_type].copy()
        
        if event_data.empty:
            st.warning(f"No {event_type.lower()} data available.")
            continue
        
        # Prepare chart data with all three metrics side by side for each time period
        chart_data = []
        for time in time_order:
            for metric_config in metrics_config:
                metric_name = metric_config['name'].lower()
                metric_row = event_data[
                    (event_data['time_display'] == time) & 
                    (event_data['metric'] == metric_name)
                ]
                
                count = metric_row['count'].iloc[0] if not metric_row.empty else 0
                chart_data.append({
                    'Time': time,
                    'Metric': metric_config['label'],
                    'Count': count,
                    'Color': metric_config['color']
                })
        
        chart_df = pd.DataFrame(chart_data)
        
        # Ensure Metric column values match exactly what we expect
        chart_df['Metric'] = chart_df['Metric'].astype(str).str.strip()
        
        # Ensure Count is numeric and handle any NaN values
        chart_df['Count'] = pd.to_numeric(chart_df['Count'], errors='coerce').fillna(0)
        
        # Create grouped (side-by-side) bar chart using xOffset for proper grouping
        # xOffset will position bars side by side within each time period (not stacked)
        # Use narrow bars and minimal padding to create tight grouping within each time period
        bar_width = 12  # Narrow bars to create tight grouping
        
        bars = alt.Chart(chart_df).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            opacity=1.0,
            width=bar_width  # Narrow bar width for tight grouping
        ).encode(
            x=alt.X('Time:O',
                   title='',
                   axis=alt.Axis(
                       labelAngle=0 if time_period == "Monthly" else -45 if time_period == "Daily" else -30,
                       labelFontSize=11,
                       titleFontSize=14,
                       labelLimit=100,
                       bandPosition=0.5  # Center bars within each band
                   ),
                   sort=time_order,
                   # For xOffset grouped bars with tight spacing:
                   # - paddingInner: spacing between time period groups (very small)
                   # - paddingOuter: spacing at chart edges (small)
                   # - Narrow bar width (12px) ensures bars are close together within each group
                   scale=alt.Scale(
                       paddingInner=0.01,  # Minimal spacing between time periods
                       paddingOuter=0.1    # Small edge spacing
                   )),
            y=alt.Y('Count:Q',
                   title='Count',
                   axis=alt.Axis(
                       format='~s',
                       titleFontSize=14,
                       labelFontSize=12,
                       grid=True,
                       gridColor='#e0e0e0'
                   ),
                   scale=alt.Scale(zero=True)),
            xOffset=alt.XOffset('Metric:N',
                               sort=['Instances', 'Visits', 'Users']),  # Order: Instances, Visits, Users
            color=alt.Color('Metric:N',
                          scale=alt.Scale(
                              domain=['Instances', 'Visits', 'Users'],
                              range=['#4A90E2', '#50C878', '#FFA726']
                          ),
                          legend=alt.Legend(
                              title="Metric Type",
                              titleFontSize=13,
                              labelFontSize=12,
                              orient='bottom'
                          ),
                          sort=['Instances', 'Visits', 'Users']),  # Ensure consistent order
            tooltip=[
                alt.Tooltip('Time:N', title='Time Period'),
                alt.Tooltip('Metric:N', title='Metric'),
                alt.Tooltip('Count:Q', title='Count', format=',')
            ]
        ).properties(
            width=chart_width,
            height=450,
            title=alt.TitleParams(
                text=event_titles[event_type],
                fontSize=18,
                fontWeight='bold',
                offset=10
            )
        )
        
        # Add data labels above bars
        labels = alt.Chart(chart_df).mark_text(
            align='center',
            baseline='bottom',
            fontSize=10,
            fontWeight='bold',
            color='#2C3E50',
            dy=-8
        ).encode(
            x=alt.X('Time:O', sort=time_order),
            xOffset=alt.XOffset('Metric:N',
                               sort=['Instances', 'Visits', 'Users']),  # Match bar order
            y=alt.Y('Count:Q'),
            text=alt.Text('Count:Q', format=',.0f')
        ).transform_filter(
            alt.datum.Count > 0
        )
        
        # Combine bars and labels
        chart = alt.layer(bars, labels).resolve_scale(
            x='shared',
            y='shared',
            color='shared'
        )
        
        chart = chart.configure_axis(
            labelFontSize=12,
            titleFontSize=14
        ).configure_view(
            strokeWidth=0
        )
        
        _render_altair_chart(chart, use_container_width=True)
    
    # Add summary statistics
    st.markdown("#### 📈 Summary Statistics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        instances_data = aggregated_df[aggregated_df['metric'] == 'instances']
        total_instances = instances_data['count'].sum() if not instances_data.empty else 0
        st.metric(
            label="⚡ Total Instances", 
            value=f"{int(total_instances):,}",
            help="Sum of instances across the selected time period"
        )
    
    with col2:
        visits_data = aggregated_df[aggregated_df['metric'] == 'visits']
        total_visits = visits_data['count'].sum() if not visits_data.empty else 0
        st.metric(
            label="🔄 Total Visits",
            value=f"{int(total_visits):,}",
            help="Sum of visits across the selected time period"
        )
    
    with col3:
        users_data = aggregated_df[aggregated_df['metric'] == 'users']
        total_users = users_data['count'].sum() if not users_data.empty else 0
        st.metric(
            label="👥 Total Users",
            value=f"{int(total_users):,}",
            help="Sum of users across the selected time period"
        )

def render_parent_poll_responses(poll_responses_df: pd.DataFrame, game_conversion_df: pd.DataFrame) -> None:
    """Render parent poll responses visualization"""
    import altair as alt
    
    if poll_responses_df.empty:
        st.warning("No parent poll responses data available.")
        return
    
    st.markdown("### 📊 Parent Poll Responses")
    
    # Get unique games for filter
    unique_games = sorted(game_conversion_df['game_name'].unique())
    
    # Add game filter - default to first game if available
    st.markdown("**🎮 Game Filter:**")
    default_games = [unique_games[0]] if len(unique_games) > 0 else []
    selected_games = st.multiselect(
        "Select Games for Parent Poll Analysis:",
        options=unique_games,
        default=default_games,  # Default to first game
        help="Select one or more games to show parent poll responses."
    )
    
    # Filter data based on selected games
    if selected_games:
        # Check if game_name column exists in poll data
        if 'game_name' in poll_responses_df.columns:
            filtered_df = poll_responses_df[poll_responses_df['game_name'].isin(selected_games)]
            if filtered_df.empty:
                st.warning(f"No poll data found for selected games: {', '.join(selected_games)}")
                return
        else:
            # Fallback for data without game_name column
            filtered_df = poll_responses_df.copy()
            st.info(f"🎮 Game filter selected: {', '.join(selected_games)} (Note: Poll data doesn't include game filtering yet)")
    else:
        filtered_df = poll_responses_df.copy()
    
    if filtered_df.empty:
        st.warning("No data available for the selected games.")
        return
    
    # Get unique questions
    unique_questions = sorted(filtered_df['question'].unique())
    
    if len(unique_questions) == 0:
        st.warning("No poll questions found in the data.")
        return
    
    # Display up to 3 questions side by side
    num_questions = min(len(unique_questions), 3)
    
    # Create columns for side-by-side display
    if num_questions == 3:
        chart_cols = st.columns(3)
    elif num_questions == 2:
        chart_cols = st.columns(2)
    else:
        chart_cols = st.columns(1)
    
    # Create charts for each question (up to 3)
    for i in range(num_questions):
        question = unique_questions[i]
        
        with chart_cols[i]:
            st.markdown(f"#### {question}")
            
            # Filter data for this question
            question_data = filtered_df[filtered_df['question'] == question].copy()
            
            if question_data.empty:
                st.warning(f"No data available for {question}")
                continue
            
            # Create bar chart
            chart = alt.Chart(question_data).mark_bar(
                cornerRadius=6,
                stroke='white',
                strokeWidth=2,
                color='#4A90E2'
            ).encode(
                x=alt.X('option:N', 
                        title='Response Option', 
                        axis=alt.Axis(
                            labelAngle=-45,
                            labelFontSize=10,
                            titleFontSize=12
                        )),
                y=alt.Y('count:Q', 
                        title='Responses', 
                        axis=alt.Axis(format='~s')),
                tooltip=['option:N', 'count:Q']
            ).properties(
                width=350,
                height=300,
                title=f'{question}'
            )
            
            # Add data labels
            labels = alt.Chart(question_data).mark_text(
                align='center',
                baseline='bottom',
                color='#2E8B57',
                fontSize=12,
                fontWeight='bold',
                dy=-10
            ).encode(
                x=alt.X('option:N'),
                y=alt.Y('count:Q'),
                text=alt.Text('count:Q', format='.0f')
            )
            
            # Combine chart and labels
            final_chart = (chart + labels).configure_axis(
                labelFontSize=12,
                titleFontSize=14,
                grid=True
            ).configure_title(
                fontSize=14,
                fontWeight='bold'
            )
            
            _render_altair_chart(final_chart, use_container_width=True)
            
            # Add summary statistics for this question
            total_responses = question_data['count'].sum()
            most_popular = question_data.loc[question_data['count'].idxmax(), 'option']
            most_popular_count = question_data['count'].max()
            
            st.metric(
                label="Total Responses",
                value=f"{total_responses:,}",
                help=f"Total number of responses for {question}"
            )
            st.caption(f"Most popular: {most_popular} ({most_popular_count:,})")
    
    # If there are more than 3 questions, show a message
    if len(unique_questions) > 3:
        st.info(f"Note: Showing first 3 of {len(unique_questions)} questions. Filter by game to see specific questions.")


def render_question_correctness_chart(question_correctness_df: pd.DataFrame) -> None:
    """Render stacked percent bar chart of Correct vs Incorrect per question for a selected game."""
    import altair as alt

    if question_correctness_df.empty:
        st.warning("No per-question correctness data available.")
        return

    st.markdown("### ✅ Question Correctness by Question Number")

    # Game selector (single-select)
    games = sorted(question_correctness_df['game_name'].dropna().unique())
    if not games:
        st.warning("No games found in correctness data.")
        return

    selected_game = st.selectbox(
        "Select a Game:",
        options=games,
        index=0,
        help="Choose a game to view per-question correctness."
    )

    df_game = question_correctness_df[question_correctness_df['game_name'] == selected_game].copy()
    if df_game.empty:
        st.warning("No data available for the selected game.")
        return

    # Ensure ordering by question number
    df_game['question_number'] = pd.to_numeric(df_game['question_number'], errors='coerce')
    df_game = df_game.dropna(subset=['question_number']).sort_values('question_number')

    # Build stacked percent chart (use precomputed percent)
    chart = alt.Chart(df_game).mark_bar().encode(
        x=alt.X('question_number:O', title='Question Number', axis=alt.Axis(labelAngle=0, labelFontSize=14, titleFontSize=16)),
        y=alt.Y('percent:Q', title='% of Users', stack='normalize'),
        color=alt.Color('correctness:N', scale=alt.Scale(domain=['Correct', 'Incorrect'], range=['#2ECC71', '#E74C3C']), legend=alt.Legend(title='Response')),
        tooltip=['question_number:O', 'correctness:N', alt.Tooltip('percent:Q', format='.1f'), 'user_count:Q', 'total_users:Q']
    ).properties(width=900, height=400, title=f'Correct vs Incorrect by Question — {selected_game}')

    _render_altair_chart(chart, use_container_width=True)

# Streamlit page config must be called before any other Streamlit command
st.set_page_config(page_title="Hybrid Dashboard", layout="wide")

def main() -> None:
    st.title("Hybrid Dashboard")
    st.caption("Performance Optimized - Using Preprocessed Data")
    
    
    # Check if processed data exists
    check_processed_data()
    
    # Try to refresh poll responses from live DB without altering other datasets
    try_refresh_poll_responses_data()

    with st.spinner("Loading data..."):
        (summary_df, game_conversion_df, time_series_df, 
         repeatability_df, score_distribution_df, poll_responses_df, question_correctness_df, metadata, processed_data_df) = load_processed_data()

    if summary_df.empty:
        st.warning("No data available.")
        return

    # Add filters
    st.markdown("### 🎮 Filters")
    
    # Date range filter for conversion funnel
    if 'date' in processed_data_df.columns and not processed_data_df.empty:
        min_date = processed_data_df['date'].min()
        max_date = processed_data_df['date'].max()
        date_range = st.date_input(
            "📅 Select Date Range for Conversion Funnel:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Select a date range to filter the conversion funnel data."
        )
    else:
        date_range = None
    
    # Game Name filter - get unique games from game_conversion_df
    unique_games = sorted(game_conversion_df['game_name'].unique())
    selected_games = st.multiselect(
        "Select Game Names to filter by:",
        options=unique_games,
        default=[],  # Empty by default - shows all games
        help="Select one or more games to filter the dashboard data. Leave empty to show all games."
    )
    
    # Show filter summary
    if not selected_games or len(selected_games) == len(unique_games):
        game_summary = "**All Games**"
        game_count = len(unique_games)
    else:
        game_summary = f"{', '.join(selected_games)}"
        game_count = len(selected_games)
    
    # Filter processed data by date range and games if specified
    # If processed_data_df is empty, use summary_df directly without filtering
    if processed_data_df.empty:
        # Use summary_df directly - no date filtering available
        filtered_summary_df = summary_df.copy()
    else:
        filtered_processed_data = processed_data_df.copy()
        
        # Apply date filter
        if date_range and isinstance(date_range, tuple) and len(date_range) == 2:
            start_date, end_date = date_range
            if start_date and end_date:
                filtered_processed_data = filtered_processed_data[
                    (filtered_processed_data['date'] >= start_date) &
                    (filtered_processed_data['date'] <= end_date)
                ]
        
        # Apply game filter
        if selected_games and len(selected_games) < len(unique_games):
            if 'game_name' in filtered_processed_data.columns:
                filtered_processed_data = filtered_processed_data[
                    filtered_processed_data['game_name'].isin(selected_games)
                ]
        
        # Calculate funnel metrics from filtered aggregated data
        if not filtered_processed_data.empty and 'event' in filtered_processed_data.columns:
            # Check if it's the new aggregated format (has instances, visits, users columns)
            if 'instances' in filtered_processed_data.columns and 'visits' in filtered_processed_data.columns and 'users' in filtered_processed_data.columns:
                # New aggregated format: sum up the metrics by event
                funnel_stages = ['started', 'introduction', 'mid_introduction', 'validation', 'questions', 'parent_poll', 'rewards', 'completed']
                filtered_summary_data = []
                for stage in funnel_stages:
                    stage_data = filtered_processed_data[filtered_processed_data['event'] == stage]
                    if not stage_data.empty:
                        filtered_summary_data.append({
                            'Event': stage,
                            'Users': stage_data['users'].sum(),
                            'Visits': stage_data['visits'].sum(),
                            'Instances': stage_data['instances'].sum()
                        })
                    else:
                        filtered_summary_data.append({
                            'Event': stage,
                            'Users': 0,
                            'Visits': 0,
                            'Instances': 0
                        })
                filtered_summary_df = pd.DataFrame(filtered_summary_data)
            else:
                # Old format: calculate from raw data (idvisitor_converted, idvisit, idlink_va)
                funnel_stages = ['started', 'introduction', 'mid_introduction', 'validation', 'questions', 'parent_poll', 'rewards', 'completed']
                filtered_summary_data = []
                for stage in funnel_stages:
                    stage_data = filtered_processed_data[filtered_processed_data['event'] == stage]
                    filtered_summary_data.append({
                        'Event': stage,
                        'Users': stage_data['idvisitor_converted'].nunique() if 'idvisitor_converted' in stage_data.columns else 0,
                        'Visits': stage_data['idvisit'].nunique() if 'idvisit' in stage_data.columns else 0,
                        'Instances': len(stage_data)
                    })
                filtered_summary_df = pd.DataFrame(filtered_summary_data)
        else:
            # Fallback to summary_df if filtered_processed_data is empty or missing event column
            filtered_summary_df = summary_df.copy()
    
    # Render conversion funnel with date and game filters applied
    render_modern_dashboard(filtered_summary_df, filtered_summary_df)
    
    # Keep the old game-specific logic for backward compatibility (but it won't be used now)
    if False:  # Disabled - using filtered_processed_data instead
        # Show filtered conversion funnel - use game-specific numbers
        selected_games_data = game_conversion_df[game_conversion_df['game_name'].isin(selected_games)]
        if not selected_games_data.empty:
            # Ensure all required columns exist
            required_cols = ['started_users', 'completed_users', 'started_visits', 'completed_visits', 'started_instances', 'completed_instances']
            for col in required_cols:
                if col not in selected_games_data.columns:
                    st.error(f"Missing required column: {col}")
                    render_modern_dashboard(summary_df, summary_df)
                    return
            
            # Aggregate the selected games - ensure numeric types
            total_started_users = pd.to_numeric(selected_games_data['started_users'], errors='coerce').fillna(0).sum()
            total_completed_users = pd.to_numeric(selected_games_data['completed_users'], errors='coerce').fillna(0).sum()
            total_started_visits = pd.to_numeric(selected_games_data['started_visits'], errors='coerce').fillna(0).sum()
            total_completed_visits = pd.to_numeric(selected_games_data['completed_visits'], errors='coerce').fillna(0).sum()
            total_started_instances = pd.to_numeric(selected_games_data['started_instances'], errors='coerce').fillna(0).sum()
            total_completed_instances = pd.to_numeric(selected_games_data['completed_instances'], errors='coerce').fillna(0).sum()
            
            # Convert to int to avoid float display issues
            total_started_users = int(total_started_users) if not pd.isna(total_started_users) else 0
            total_completed_users = int(total_completed_users) if not pd.isna(total_completed_users) else 0
            total_started_visits = int(total_started_visits) if not pd.isna(total_started_visits) else 0
            total_completed_visits = int(total_completed_visits) if not pd.isna(total_completed_visits) else 0
            total_started_instances = int(total_started_instances) if not pd.isna(total_started_instances) else 0
            total_completed_instances = int(total_completed_instances) if not pd.isna(total_completed_instances) else 0
            
            # Create summary data for selected games with explicit dtype and column order
            selected_games_summary = pd.DataFrame({
                'Event': ['Started', 'Completed'],
                'Users': [total_started_users, total_completed_users],
                'Visits': [total_started_visits, total_completed_visits],
                'Instances': [total_started_instances, total_completed_instances]
            })
            # Ensure numeric columns are properly typed
            selected_games_summary['Users'] = pd.to_numeric(selected_games_summary['Users'], errors='coerce').fillna(0).astype(int)
            selected_games_summary['Visits'] = pd.to_numeric(selected_games_summary['Visits'], errors='coerce').fillna(0).astype(int)
            selected_games_summary['Instances'] = pd.to_numeric(selected_games_summary['Instances'], errors='coerce').fillna(0).astype(int)
            
            # Verify the DataFrame structure before rendering
            if selected_games_summary.empty or 'Event' not in selected_games_summary.columns:
                st.error("Error creating filtered conversion data. Showing all games instead.")
                render_modern_dashboard(summary_df, summary_df)
            else:
                render_modern_dashboard(selected_games_summary, selected_games_summary)
        else:
            st.warning("No data found for selected games.")
    
    # Add Time Series Analysis - right after conversion funnels
    st.markdown("---")
    st.markdown("## 📈 Time-Series Analysis")
    
    if not time_series_df.empty:
        render_time_series_analysis(time_series_df, game_conversion_df)
    else:
        st.warning("No time series data available.")
    
    # Add Score Distribution Analysis
    st.markdown("---")
    st.markdown("## 🎯 Score Distribution Analysis")
    
    if not score_distribution_df.empty:
        render_score_distribution_chart(score_distribution_df)
    else:
        st.warning("No score distribution data available.")
    
    # Add Parent Poll Responses Analysis
    st.markdown("---")
    st.markdown("## 📊 Parent Poll Responses Analysis")
    
    if not poll_responses_df.empty:
        render_parent_poll_responses(poll_responses_df, game_conversion_df)
    else:
        st.warning("No parent poll responses data available.")
    
    # Add Question Correctness by Question Number Analysis
    st.markdown("---")
    st.markdown("## ✅ Question Correctness by Question Number")
    
    if not question_correctness_df.empty:
        render_question_correctness_chart(question_correctness_df)
    else:
        st.warning("No question correctness data available. Please run preprocess_data.py to generate the data.")
    
    # Add Repeatability Analysis
    st.markdown("---")
    st.markdown("## 🎮 Game Repeatability Analysis")
    
    if not repeatability_df.empty:
        render_repeatability_analysis(repeatability_df)
    else:
        st.warning("No repeatability data available.")


# Streamlit automatically runs the script, so call main() directly
main()