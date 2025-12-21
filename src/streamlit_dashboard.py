# ⚠️ All data used in this dashboard must be preprocessed using master_processor.py before deployment.
# This dashboard only handles visualization of preprocessed data to stay within Render's 512MB memory limit.
# Updated: Using game_conversion_numbers.csv for optimized game filtering (commit 90ecd9a)

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
        
        # Load conversion funnel raw data - MUST load raw file with language column
        # Priority: 1) Root conversion_funnel.csv (raw with language), 2) data/conversion_funnel.csv, 3) fallback to processed_data
        conversion_funnel_df = None
        conversion_funnel_path = None
        
        # Check root directory first (most likely to have raw data with language)
        root_path = "conversion_funnel.csv"
        data_path = os.path.join(DATA_DIR, "conversion_funnel.csv")
        
        # Try root directory first
        if os.path.exists(root_path):
            try:
                sample = pd.read_csv(root_path, nrows=5, low_memory=False)
                # Check if it's raw data (has idlink_va/idvisitor) AND has language
                is_raw = 'idlink_va' in sample.columns or 'idvisitor' in sample.columns
                has_language = 'language' in sample.columns
                
                if is_raw and has_language:
                    # This is the raw file with language - use it!
                    conversion_funnel_path = root_path
                    conversion_funnel_df = pd.read_csv(conversion_funnel_path, low_memory=False)
                elif is_raw:
                    # Raw file but no language - still use it (might have language in full data)
                    conversion_funnel_path = root_path
                    conversion_funnel_df = pd.read_csv(conversion_funnel_path, low_memory=False)
            except Exception as e:
                pass
        
        # If root file didn't work, try data/ directory
        if conversion_funnel_df is None and os.path.exists(data_path):
            try:
                sample = pd.read_csv(data_path, nrows=5, low_memory=False)
                is_raw = 'idlink_va' in sample.columns or 'idvisitor' in sample.columns
                has_language = 'language' in sample.columns
                
                if is_raw and has_language:
                    conversion_funnel_path = data_path
                    conversion_funnel_df = pd.read_csv(conversion_funnel_path, low_memory=False)
                elif is_raw:
                    conversion_funnel_path = data_path
                    conversion_funnel_df = pd.read_csv(conversion_funnel_path, low_memory=False)
            except Exception:
                pass
        
        # If we loaded a file, process it
        if conversion_funnel_df is not None:
            # Ensure date column is properly formatted if it exists
            if 'date' in conversion_funnel_df.columns:
                conversion_funnel_df['date'] = pd.to_datetime(conversion_funnel_df['date']).dt.date
            elif 'server_time' in conversion_funnel_df.columns:
                # Convert server_time to date if date column doesn't exist
                conversion_funnel_df['date'] = pd.to_datetime(conversion_funnel_df['server_time']).dt.date
            
            # Ensure idvisitor_converted exists if we have idvisitor (for raw data)
            if 'idvisitor' in conversion_funnel_df.columns and 'idvisitor_converted' not in conversion_funnel_df.columns:
                conversion_funnel_df['idvisitor_converted'] = conversion_funnel_df['idvisitor']
            
            # Only process numeric columns if this is aggregated data (has instances/visits/users)
            # Raw data won't have these columns
            if 'instances' in conversion_funnel_df.columns:
                for col in ['instances', 'visits', 'users']:
                    if col in conversion_funnel_df.columns:
                        conversion_funnel_df[col] = pd.to_numeric(conversion_funnel_df[col], errors='coerce').fillna(0).astype(int)
        else:
            # Fallback to processed_data_df if conversion_funnel.csv doesn't exist
            conversion_funnel_df = processed_data_df.copy()
            conversion_funnel_path = None
        
        # Load time series data
        time_series_df = pd.read_csv(os.path.join(DATA_DIR, "time_series_data.csv"))
        
        # Extract domain from game_code if available (for time series data)
        if 'game_code' in time_series_df.columns and 'domain' not in time_series_df.columns:
            def extract_domain_from_game_code(game_code):
                """Extract domain from game_code (e.g., HY-29-LL-06 -> LL)"""
                if pd.isna(game_code) or not isinstance(game_code, str):
                    return None
                parts = game_code.split('-')
                # Pattern: HY-29-LL-06 -> domain is LL (3rd element, index 2)
                # Split by '-': ['HY', '29', 'LL', '06'] -> parts[2] = 'LL'
                if len(parts) >= 3:
                    return parts[2]
                return None
            time_series_df['domain'] = time_series_df['game_code'].apply(extract_domain_from_game_code)
        
        # Load repeatability data
        repeatability_df = pd.read_csv(os.path.join(DATA_DIR, "repeatability_data.csv"))
        
        # Load score distribution data
        score_distribution_df = pd.read_csv(os.path.join(DATA_DIR, "score_distribution_data.csv"))
        
        # Extract domain from game_code if available (similar to question_correctness_df)
        if 'game_code' in score_distribution_df.columns and 'domain' not in score_distribution_df.columns:
            def extract_domain_from_game_code(game_code):
                """Extract domain from game_code (e.g., HY-29-LL-06 -> LL)"""
                if pd.isna(game_code) or not isinstance(game_code, str):
                    return None
                parts = game_code.split('-')
                # Pattern: HY-29-LL-06 -> domain is LL (3rd element, index 2)
                # Split by '-': ['HY', '29', 'LL', '06'] -> parts[2] = 'LL'
                if len(parts) >= 3:
                    return parts[2]
                return None
            score_distribution_df['domain'] = score_distribution_df['game_code'].apply(extract_domain_from_game_code)
        
        # Load poll responses data
        poll_responses_df = pd.read_csv(os.path.join(DATA_DIR, "poll_responses_data.csv"))
        
        # Extract domain from game_code if available
        if 'game_code' in poll_responses_df.columns and 'domain' not in poll_responses_df.columns:
            def extract_domain_from_game_code(game_code):
                """Extract domain from game_code (e.g., HY-29-LL-06 -> LL)"""
                if pd.isna(game_code) or not isinstance(game_code, str):
                    return None
                parts = game_code.split('-')
                # Pattern: HY-29-LL-06 -> domain is LL (3rd element, index 2)
                # Split by '-': ['HY', '29', 'LL', '06'] -> parts[2] = 'LL'
                if len(parts) >= 3:
                    return parts[2]
                return None
            poll_responses_df['domain'] = poll_responses_df['game_code'].apply(extract_domain_from_game_code)

        # Load per-question correctness data (optional)
        qpath = os.path.join(DATA_DIR, "question_correctness_data.csv")
        if os.path.exists(qpath):
            question_correctness_df = pd.read_csv(qpath)
            
            # Extract domain from game_code if available (similar to poll_responses_df)
            if 'game_code' in question_correctness_df.columns and 'domain' not in question_correctness_df.columns:
                def extract_domain_from_game_code(game_code):
                    """Extract domain from game_code (e.g., HY-29-LL-06 -> LL)"""
                    if pd.isna(game_code) or not isinstance(game_code, str):
                        return None
                    parts = game_code.split('-')
                    # Pattern: HY-29-LL-06 -> domain is LL (3rd element, index 2)
                    # Split by '-': ['HY', '29', 'LL', '06'] -> parts[2] = 'LL'
                    if len(parts) >= 3:
                        return parts[2]
                    return None
                question_correctness_df['domain'] = question_correctness_df['game_code'].apply(extract_domain_from_game_code)
        else:
            question_correctness_df = pd.DataFrame()
        
        # Load video viewership data (optional)
        vpath = os.path.join(DATA_DIR, "video_viewership_data.csv")
        if os.path.exists(vpath):
            video_viewership_df = pd.read_csv(vpath)
            # Ensure numeric columns are properly typed
            numeric_cols = ['Started', 'Questions', 'Rewards', 'Video Started', 'Average', 'Min', 'Max']
            for col in numeric_cols:
                if col in video_viewership_df.columns:
                    video_viewership_df[col] = pd.to_numeric(video_viewership_df[col], errors='coerce').fillna(0)
        else:
            video_viewership_df = pd.DataFrame()
        
        # Create metadata
        metadata = {
            'last_updated': datetime.now().isoformat(),
            'data_source': 'data',
            'version': '2.0'
        }
        
        return (summary_df, game_conversion_df, time_series_df, 
                repeatability_df, score_distribution_df, poll_responses_df, question_correctness_df, video_viewership_df, metadata, processed_data_df, conversion_funnel_df, conversion_funnel_path)
    
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
    # Funnel stages order: started, introduction, questions, mid_introduction, validation, parent_poll, rewards, completed
    funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
    stage_labels = {
        'started': 'Started',
        'introduction': 'Introduction',
        'mid_introduction': 'Mid Introduction',
        'parent_poll': 'Parent Poll',
        'validation': 'Validation',  # validation event (from question_completed) displays as "Validation"
        'rewards': 'Rewards',
        'questions': 'Questions',  # questions event (from action_completed) displays as "Questions"
        'completed': 'Completed'
    }
    
    if 'Event' in conversion_df.columns:
        # Summary data format (total data)
        # Extract data for all funnel stages
        funnel_data = {}
        for stage in funnel_stages:
            # Match event (case-insensitive, handle whitespace)
            stage_row = conversion_df[conversion_df['Event'].astype(str).str.strip().str.lower() == stage.lower().strip()]
            try:
                if not stage_row.empty and len(stage_row) > 0:
                    # Direct access to the first matching row - use direct column access
                    # Check for both capitalized and lowercase column names
                    if 'Users' in stage_row.columns:
                        users_val = stage_row['Users'].iloc[0]
                    elif 'users' in stage_row.columns:
                        users_val = stage_row['users'].iloc[0]
                    else:
                        users_val = 0
                    
                    if 'Visits' in stage_row.columns:
                        visits_val = stage_row['Visits'].iloc[0]
                    elif 'visits' in stage_row.columns:
                        visits_val = stage_row['visits'].iloc[0]
                    else:
                        visits_val = 0
                    
                    if 'Instances' in stage_row.columns:
                        instances_val = stage_row['Instances'].iloc[0]
                    elif 'instances' in stage_row.columns:
                        instances_val = stage_row['instances'].iloc[0]
                    else:
                        instances_val = 0
                    
                    # Convert to numeric and handle NaN for scalars
                    users_num = pd.to_numeric(users_val, errors='coerce')
                    visits_num = pd.to_numeric(visits_val, errors='coerce')
                    instances_num = pd.to_numeric(instances_val, errors='coerce')
                    
                    # Handle NaN values (pd.isna works on scalars)
                    funnel_data[f'{stage}_users'] = int(users_num if not pd.isna(users_num) else 0)
                    funnel_data[f'{stage}_visits'] = int(visits_num if not pd.isna(visits_num) else 0)
                    funnel_data[f'{stage}_instances'] = int(instances_num if not pd.isna(instances_num) else 0)
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
    
    
    # Add selection option to show only one funnel at a time
    funnel_type = st.radio(
        "Select Conversion Funnel Type:",
        options=['Instances', 'Visits', 'Users'],
        horizontal=True,
        help="Select which conversion funnel to display"
    )
    
    # Create a single funnel based on selection
    if funnel_type == 'Instances':
        funnel_title = "⚡ Instances Funnel"
        funnel_color = '#F5A623'
        data_key = 'instances'
    elif funnel_type == 'Visits':
        funnel_title = "🔄 Visits Funnel"
        funnel_color = '#7ED321'
        data_key = 'visits'
    else:  # Users
        funnel_title = "👥 Users Funnel"
        funnel_color = '#4A90E2'
        data_key = 'users'
    
    st.markdown(f"#### {funnel_title}")
    
    # Add note about Parent Poll being optional
    st.info("ℹ️ **Note:** Parent Poll is an optional step in the conversion funnel.")
    
    # Create data for selected funnel
    funnel_data_df = pd.DataFrame([
        {
            'Stage': stage_labels[stage], 
            'Count': funnel_data.get(f'{stage}_{data_key}', 0), 
            'Order': idx,
            'IsOptional': (stage == 'parent_poll')  # Mark parent_poll as optional
        }
            for idx, stage in enumerate(funnel_stages)
        ])
        
    # Define lighter colors for parent_poll (optional step)
    lighter_colors = {
        '#F5A623': '#F9C866',  # Lighter orange for Instances
        '#7ED321': '#B3E57A',  # Lighter green for Visits
        '#4A90E2': '#7DB3F0'   # Lighter blue for Users
    }
    lighter_color = lighter_colors.get(funnel_color, '#CCCCCC')  # Default lighter gray if color not found
    
    funnel_chart = alt.Chart(funnel_data_df).mark_bar(
            cornerRadius=6,
            stroke='white',
        strokeWidth=2
        ).encode(
            x=alt.X('Count:Q', title='Count', axis=alt.Axis(format='~s')),
        y=alt.Y('Stage:N', 
               sort=alt.SortField(field='Order', order='ascending'), 
               title='',
               axis=alt.Axis(
                   labelLimit=200,  # Allow longer labels to display fully
                   labelFontSize=22
               )),
        color=alt.condition(
            alt.datum.IsOptional,
            alt.value(lighter_color),  # Lighter color for optional (parent_poll)
            alt.value(funnel_color)  # Normal color for others
        ),
            opacity=alt.condition(
                alt.datum.Stage == 'Started',
                alt.value(0.8),
                alt.value(1.0)
            ),
            tooltip=['Stage:N', 'Count:Q']
        ).properties(
        width=800,
            height=600  # Increased height for 8 stages
        )
        
        # Add labels with complete numbers - positioned at the start of bars
    funnel_labels = alt.Chart(funnel_data_df).mark_text(
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
        
    funnel_display = alt.layer(funnel_chart, funnel_labels).configure_view(
            strokeWidth=0
        ).configure_axis(
            labelFontSize=22,
            titleFontSize=24,
            grid=False
        )
        
    _render_altair_chart(funnel_display, use_container_width=True)
    
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
    
def render_score_distribution_chart(score_distribution_df: pd.DataFrame, selected_games: list) -> None:
    """Render score distribution chart"""
    import altair as alt
    
    
    if score_distribution_df.empty:
        st.warning("No score distribution data available.")
        return
    
    # Use global game filter - function receives selected_games as parameter
    # Data is already filtered by global filters, but we need to ensure at least one game is selected
    if not selected_games:
        st.warning("Please select at least one game in the global filters above.")
        return
    
    # Show which games are being displayed
    available_games = sorted(score_distribution_df['game_name'].unique())
    display_games = [g for g in selected_games if g in available_games]
    if display_games:
        if len(display_games) == 1:
            st.info(f"📊 Showing score distribution for: **{display_games[0]}**")
        else:
            st.info(f"📊 Showing score distribution for: **{', '.join(display_games)}** ({len(display_games)} games)")
    
    # Filter data based on global game filter (already applied, but double-check)
    filtered_df = score_distribution_df[score_distribution_df['game_name'].isin(selected_games)] if selected_games else score_distribution_df
    
    if filtered_df.empty:
        st.warning("No data available for the selected games.")
        return
    
    # Create the score distribution chart
    st.markdown("### 📊 Score Distribution")
    if len(display_games) == 1:
        st.markdown(f"This chart shows how many users achieved each total score for **{display_games[0]}**.")
    else:
        st.markdown(f"This chart shows how many users achieved each total score for the selected games ({len(display_games)} games).")
    
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

def render_time_series_analysis(time_series_df: pd.DataFrame, game_conversion_df: pd.DataFrame, selected_games: list = None) -> None:
    """Render time series analysis with a single chart showing Started and Completed for selected metric"""
    import altair as alt
    
    if time_series_df.empty:
        st.warning("No time series data available.")
        return
    
    st.markdown("### 📈 Time-Series Analysis")
    st.info("ℹ️ This section uses the global game, domain, and language filters from above.")
    
    # Show which games are being displayed
    # Get available games from the data (excluding "All Games")
    available_games = sorted([g for g in time_series_df['game_name'].unique() if g != 'All Games'])
    
    if selected_games and len(selected_games) > 0:
        # Show selected games if any are available in the data
        display_games = [g for g in selected_games if g in available_games]
        if display_games:
            if len(display_games) == 1:
                st.info(f"📊 Showing time series for: **{display_games[0]}**")
            else:
                st.info(f"📊 Showing time series for: **{', '.join(display_games)}** ({len(display_games)} games)")
    elif not selected_games or len(selected_games) == 0:
        # Show "All Games" when no specific games are selected
        if 'All Games' in time_series_df['game_name'].unique():
            st.info("📊 Showing time series for: **All Games**")
    
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
        # Metric selection (Instances, Visits, Users)
        selected_metric = st.radio(
            "Select Metric:",
            options=["Instances", "Visits", "Users"],
            horizontal=True,
            help="Select which metric to display (Instances, Visits, or Users)"
        )
    
    # Filter by selected time period
    period_type_map = {"Daily": "Day", "Weekly": "Week", "Monthly": "Month"}
    period_filter = period_type_map.get(time_period, time_period)
    filtered_ts_df = time_series_df[time_series_df['period_type'] == period_filter].copy()
    
    # Use global filters (already applied to time_series_df via filtered_time_series_df)
    # Filter by game if global game filter is active
    if filtered_ts_df.empty:
        st.warning("No data available for the selected filters.")
        return
    
    # Check if we have game-specific data or "All Games" data
    has_game_specific_data = filtered_ts_df[filtered_ts_df['game_name'] != 'All Games'].shape[0] > 0
    
    if has_game_specific_data:
        # Separate RM active users (which doesn't have game-specific data)
        rm_data = filtered_ts_df[
            (filtered_ts_df['game_name'] == 'All Games') & 
            (filtered_ts_df['metric'] == 'rm_active_users')
        ].copy()
        
        # Filter game-specific data (already filtered by global filters)
        game_data = filtered_ts_df[filtered_ts_df['game_name'] != 'All Games'].copy()
        
        # Aggregate across selected games
        if not game_data.empty:
            aggregated_game_df = game_data.groupby(['period_label', 'metric', 'event']).agg({'count': 'sum'}).reset_index()
            aggregated_game_df['game_name'] = 'Selected Games'
        else:
            aggregated_game_df = pd.DataFrame(columns=['period_label', 'metric', 'event', 'count', 'game_name'])
        
        # Combine RM data with game data
        if not rm_data.empty:
            rm_data = rm_data[['period_label', 'metric', 'event', 'count']].copy()
            rm_data['game_name'] = 'All Games'
            aggregated_df = pd.concat([aggregated_game_df, rm_data], ignore_index=True)
        else:
            aggregated_df = aggregated_game_df
    else:
        # Use "All Games" data (includes RM active users)
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
            # Filter out Week 25
            time_order_df = time_order_df[time_order_df['week'] != 25]
            time_order = time_order_df['time_display'].tolist()
        except:
            time_order = sorted(aggregated_df['time_display'].unique().tolist())
            # Filter out Week 25 even if extraction fails
            time_order = [t for t in time_order if 'Week 25' not in str(t)]
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
    
    # Filter data for selected metric and include RM active users
    metric_name_lower = selected_metric.lower()
    # Include both the selected metric and RM active users (if available)
    filtered_metric_df = aggregated_df[
        (aggregated_df['metric'] == metric_name_lower) | 
        (aggregated_df['metric'] == 'rm_active_users')
    ].copy()
    
    # Filter out Week 25 from weekly view
    if time_period == "Weekly":
        filtered_metric_df = filtered_metric_df[~filtered_metric_df['time_display'].str.contains('Week 25', na=False)]
    
    if filtered_metric_df.empty:
        st.warning(f"No {selected_metric.lower()} data available for the selected time period.")
        return
    
    # Calculate dynamic width based on number of time periods
    num_periods = len(time_order)
    base_width_per_period = 100
    if time_period == "Daily":
        chart_width = max(900, num_periods * base_width_per_period)
    elif time_period == "Weekly":
        chart_width = max(800, num_periods * base_width_per_period)
    else:  # Monthly
        chart_width = max(700, num_periods * base_width_per_period)
    
    # Define metric color
    metric_colors = {
        'Instances': '#4A90E2',
        'Visits': '#50C878',
        'Users': '#FFA726'
    }
    selected_color = metric_colors.get(selected_metric, '#4A90E2')
    
    # Check if RM active users data exists
    rm_data_exists = 'rm_active_users' in filtered_metric_df['metric'].values if not filtered_metric_df.empty else False
    
    # Create single chart showing RM Active Users, Started and Completed for selected metric
    if rm_data_exists:
        st.markdown(f"### 📊 Time Series Analysis: {selected_metric} - RM Active Users, Started vs Completed")
    else:
        st.markdown(f"### 📊 Time Series Analysis: {selected_metric} - Started vs Completed")
        
    # Prepare chart data with RM Active Users, Started and Completed for each time period
    chart_data = []
    for time in time_order:
        # Add RM Active Users first if it exists
        if rm_data_exists:
            rm_row = filtered_metric_df[
                (filtered_metric_df['time_display'] == time) & 
                (filtered_metric_df['metric'] == 'rm_active_users') &
                (filtered_metric_df['event'] == 'RM Active Users')
            ]
            rm_count = rm_row['count'].iloc[0] if not rm_row.empty else 0
            chart_data.append({
                'Time': time,
                'Event': 'RM Active Users',
                'Count': rm_count
            })
        
        # Add Started and Completed
        for event_type in ['Started', 'Completed']:
            event_row = filtered_metric_df[
                (filtered_metric_df['time_display'] == time) & 
                (filtered_metric_df['event'] == event_type)
                ]
                
            count = event_row['count'].iloc[0] if not event_row.empty else 0
            chart_data.append({
                'Time': time,
                'Event': event_type,
                'Count': count
            })
        
    chart_df = pd.DataFrame(chart_data)
        
    # Ensure Count is numeric and handle any NaN values
    chart_df['Count'] = pd.to_numeric(chart_df['Count'], errors='coerce').fillna(0)
        
    # Create grouped (side-by-side) bar chart
    bar_width = 20  # Bar width for Started/Completed grouping
        
    bars = alt.Chart(chart_df).mark_bar(
            cornerRadius=6,
            stroke='white',
            strokeWidth=2,
            opacity=1.0,
        width=bar_width
        ).encode(
            x=alt.X('Time:O',
                   title='',
                   axis=alt.Axis(
                       labelAngle=0 if time_period == "Monthly" else -45 if time_period == "Daily" else -30,
                       labelFontSize=11,
                       titleFontSize=14,
                       labelLimit=100,
                   bandPosition=0.5
                   ),
                   sort=time_order,
                   scale=alt.Scale(
                   paddingInner=0.2 if time_period == "Monthly" else (0.05 if time_period == "Daily" else 0.1),
                   paddingOuter=0.1 if time_period == "Monthly" else (0.05 if time_period == "Daily" else 0.1)
                   )),
            y=alt.Y('Count:Q',
               title=f'{selected_metric} Count',
                   axis=alt.Axis(
                       format='~s',
                       titleFontSize=14,
                       labelFontSize=12,
                       grid=True,
                       gridColor='#e0e0e0'
                   ),
                   scale=alt.Scale(zero=True)),
        xOffset=alt.XOffset('Event:N',
                           sort=['RM Active Users', 'Started', 'Completed'] if rm_data_exists else ['Started', 'Completed'],
                           scale=alt.Scale(
                               paddingInner=0.1 if time_period == "Monthly" else (0.1 if time_period == "Daily" else 0.2)
                           )),
        color=alt.Color('Event:N',
                          scale=alt.Scale(
                          domain=['RM Active Users', 'Started', 'Completed'] if rm_data_exists else ['Started', 'Completed'],
                          range=['#FF6B6B', '#4A90E2', '#50C878'] if rm_data_exists else ['#4A90E2', '#50C878']
                          ),
                          legend=alt.Legend(
                          title="Event Type",
                              titleFontSize=13,
                              labelFontSize=12,
                              orient='bottom'
                          ),
                      sort=['RM Active Users', 'Started', 'Completed'] if rm_data_exists else ['Started', 'Completed']),
            tooltip=[
                alt.Tooltip('Time:N', title='Time Period'),
            alt.Tooltip('Event:N', title='Event'),
                alt.Tooltip('Count:Q', title='Count', format=',')
            ]
        ).properties(
            width=chart_width,
            height=450,
            title=alt.TitleParams(
            text=f'{selected_metric} - RM Active Users, Started vs Completed' if rm_data_exists else f'{selected_metric} - Started vs Completed',
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
        xOffset=alt.XOffset('Event:N',
                           sort=['RM Active Users', 'Started', 'Completed'] if rm_data_exists else ['Started', 'Completed'],
                           scale=alt.Scale(
                               paddingInner=0.1 if time_period == "Monthly" else (0.1 if time_period == "Daily" else 0.2)
                           )),
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
    
    # Add summary statistics for selected metric
    st.markdown("#### 📈 Summary Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        started_data = filtered_metric_df[filtered_metric_df['event'] == 'Started']
        total_started = started_data['count'].sum() if not started_data.empty else 0
        metric_icon = "⚡" if selected_metric == "Instances" else "🔄" if selected_metric == "Visits" else "👥"
        st.metric(
            label=f"{metric_icon} Total {selected_metric} (Started)", 
            value=f"{int(total_started):,}",
            help=f"Sum of {selected_metric.lower()} started across the selected time period"
        )
    
    with col2:
        completed_data = filtered_metric_df[filtered_metric_df['event'] == 'Completed']
        total_completed = completed_data['count'].sum() if not completed_data.empty else 0
        st.metric(
            label=f"{metric_icon} Total {selected_metric} (Completed)",
            value=f"{int(total_completed):,}",
            help=f"Sum of {selected_metric.lower()} completed across the selected time period"
        )

def _get_filtered_summary(summary_df: pd.DataFrame, selected_domains: list, selected_languages: list, 
                          has_domain_filter: bool, has_language_filter: bool) -> pd.DataFrame:
    """Get filtered summary using pre-calculated combinations from summary_data.csv
    
    Combinations available:
    - Overall totals: domain='All', language='All'
    - By domain only: domain='CG', language='All'
    - By language only: domain='All', language='hi'
    - By both: domain='CG', language='hi'
    """
    if summary_df.empty:
        return pd.DataFrame(columns=['Event', 'Users', 'Visits', 'Instances'])
    
    # Determine which combination to use
    if not has_domain_filter and not has_language_filter:
        # No filters - use overall totals (All, All)
        filtered_df = summary_df[
            (summary_df['domain'] == 'All') & (summary_df['language'] == 'All')
        ].copy()
    elif has_domain_filter and not has_language_filter:
        # Domain filter only - use (domain, All) combinations
        if len(selected_domains) == 1:
            # Single domain - use exact combination
            filtered_df = summary_df[
                (summary_df['domain'] == selected_domains[0]) & (summary_df['language'] == 'All')
            ].copy()
        else:
            # Multiple domains - aggregate (domain, All) rows
            filtered_df = summary_df[
                (summary_df['domain'].isin(selected_domains)) & (summary_df['language'] == 'All')
            ].copy()
            if not filtered_df.empty:
                filtered_df = filtered_df.groupby('Event').agg({
                    'Users': 'sum',
                    'Visits': 'sum',
                    'Instances': 'sum'
                }).reset_index()
    elif not has_domain_filter and has_language_filter:
        # Language filter only - use (All, language) combinations
        if len(selected_languages) == 1:
            # Single language - use exact combination
            filtered_df = summary_df[
                (summary_df['domain'] == 'All') & (summary_df['language'] == selected_languages[0])
            ].copy()
        else:
            # Multiple languages - aggregate (All, language) rows
            filtered_df = summary_df[
                (summary_df['domain'] == 'All') & (summary_df['language'].isin(selected_languages))
            ].copy()
            if not filtered_df.empty:
                filtered_df = filtered_df.groupby('Event').agg({
                    'Users': 'sum',
                    'Visits': 'sum',
                    'Instances': 'sum'
                }).reset_index()
    else:
        # Both domain and language filters - use (domain, language) combinations
        if len(selected_domains) == 1 and len(selected_languages) == 1:
            # Single domain and language - use exact combination
            filtered_df = summary_df[
                (summary_df['domain'] == selected_domains[0]) & 
                (summary_df['language'] == selected_languages[0])
            ].copy()
        else:
            # Multiple domains or languages - aggregate matching rows
            filtered_df = summary_df[
                (summary_df['domain'].isin(selected_domains)) & 
                (summary_df['language'].isin(selected_languages))
            ].copy()
            if not filtered_df.empty:
                groupby_cols = ['Event']
                filtered_df = filtered_df.groupby(groupby_cols).agg({
                    'Users': 'sum',
                    'Visits': 'sum',
                    'Instances': 'sum'
                }).reset_index()
    
    # Ensure all events exist
    if filtered_df.empty:
        all_events = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
        filtered_df = pd.DataFrame({
            'Event': all_events,
            'Users': [0] * 8,
            'Visits': [0] * 8,
            'Instances': [0] * 8
        })
    else:
        # Ensure all events are present
        all_events = pd.DataFrame({'Event': ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']})
        filtered_df = all_events.merge(filtered_df, on='Event', how='left').fillna(0)
    
    # Convert to int
    for col in ['Users', 'Visits', 'Instances']:
        if col in filtered_df.columns:
            filtered_df[col] = filtered_df[col].astype(int)
    
    return filtered_df[['Event', 'Users', 'Visits', 'Instances']]


def render_parent_poll_responses(poll_responses_df: pd.DataFrame, game_conversion_df: pd.DataFrame, 
                                 selected_games: list, selected_domains: list, selected_languages: list,
                                 has_domain_filter: bool, has_language_filter: bool) -> None:
    """Render parent poll responses visualization"""
    import altair as alt
    
    if poll_responses_df.empty:
        st.warning("No parent poll responses data available.")
        return
    
    st.markdown("### 📊 Parent Poll Responses")
    st.info("ℹ️ This section uses the global game, domain, and language filters from above.")
    
    # Apply filters to poll data - use pre-calculated combinations
    # Follow the same filter logic as conversion funnel:
    # No filters → (game_code='All', language='All')
    # Domain only → (game_code=selected, language='All')
    # Language only → (game_code='All', language=selected)
    # Both → (game_code=selected, language=selected)
    filtered_df = poll_responses_df.copy()
    
    # Check if game filter is applied
    has_game_filter = bool(selected_games)
    
    # Apply game filter if games are selected (optional - not required)
    if selected_games and 'game_name' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['game_name'].isin(selected_games)]
    
    # Determine which column to use for domain/game_code filtering
    # Prefer 'game_code' if available (like conversion funnel), otherwise use 'domain'
    domain_col = 'game_code' if 'game_code' in filtered_df.columns else 'domain'
    
    # Use pre-calculated combinations based on global domain and language filters
    # The data has all combinations: (All, All), (game_code/domain, All), (All, language), (game_code/domain, language)
    # IMPORTANT: When domain/language filters are applied WITHOUT game filter, we need to aggregate across all games
    if domain_col in filtered_df.columns and 'language' in filtered_df.columns:
        # Determine which combination to use based on global filters (same logic as conversion funnel)
        if not has_domain_filter and not has_language_filter:
            # No filters - use overall totals (game_code='All', language='All')
            filtered_df = filtered_df[
                (filtered_df[domain_col] == 'All') & (filtered_df['language'] == 'All')
            ]
            # If no game filter, aggregate across all games
            if not has_game_filter and 'game_name' in filtered_df.columns:
                filtered_df = filtered_df.groupby(['question', 'option'])['count'].sum().reset_index()
        elif has_domain_filter and not has_language_filter:
            # Domain filter only - use (game_code=selected, language='All') combinations
                filtered_df = filtered_df[
                (filtered_df[domain_col].isin(selected_domains)) & (filtered_df['language'] == 'All')
                ]
            # Aggregate: if game filter applied, keep game_name; otherwise aggregate across all games
            if has_game_filter:
                # Keep game_name in groupby when game filter is applied
                groupby_cols = ['game_name', 'question', 'option']
                if len(selected_domains) > 1:
                    groupby_cols.append(domain_col)
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
            else:
                # Aggregate across all games for the selected domain(s)
                groupby_cols = ['question', 'option']
                if len(selected_domains) > 1:
                    groupby_cols.append(domain_col)
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
                if len(selected_domains) == 1:
                    filtered_df[domain_col] = selected_domains[0]
                filtered_df['language'] = 'All'
        elif not has_domain_filter and has_language_filter:
            # Language filter only - use (game_code='All', language=selected) combinations
                filtered_df = filtered_df[
                (filtered_df[domain_col] == 'All') & (filtered_df['language'].isin(selected_languages))
            ]
            # Aggregate: if game filter applied, keep game_name; otherwise aggregate across all games
            if has_game_filter:
                # Keep game_name in groupby when game filter is applied
                groupby_cols = ['game_name', 'question', 'option']
                if len(selected_languages) > 1:
                    groupby_cols.append('language')
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
            else:
                # Aggregate across all games for the selected language(s)
                groupby_cols = ['question', 'option']
                if len(selected_languages) > 1:
                    groupby_cols.append('language')
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
                if len(selected_languages) == 1:
                    filtered_df['language'] = selected_languages[0]
            filtered_df[domain_col] = 'All'
        else:
            # Both domain and language filters - use (game_code=selected, language=selected) combinations
                filtered_df = filtered_df[
                (filtered_df[domain_col].isin(selected_domains)) & 
                    (filtered_df['language'].isin(selected_languages))
                ]
            # Aggregate: if game filter applied, keep game_name; otherwise aggregate across all games
            if has_game_filter:
                # Keep game_name in groupby when game filter is applied
                groupby_cols = ['game_name', 'question', 'option']
                if len(selected_domains) > 1:
                    groupby_cols.append(domain_col)
                if len(selected_languages) > 1:
                    groupby_cols.append('language')
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
            else:
                # Aggregate across all games for the selected domain(s) and language(s)
                groupby_cols = ['question', 'option']
                if len(selected_domains) > 1:
                    groupby_cols.append(domain_col)
                if len(selected_languages) > 1:
                    groupby_cols.append('language')
                filtered_df = filtered_df.groupby(groupby_cols)['count'].sum().reset_index()
                # Fill in fixed values for single selections
                if len(selected_domains) == 1:
                filtered_df[domain_col] = selected_domains[0]
                if len(selected_languages) == 1:
                    filtered_df['language'] = selected_languages[0]
    
    if filtered_df.empty:
        # Provide more specific message based on what filters were applied
        if has_domain_filter or has_language_filter:
            filter_desc = []
            if has_domain_filter:
                filter_desc.append(f"domain(s): {', '.join(selected_domains)}")
            if has_language_filter:
                filter_desc.append(f"language(s): {', '.join(selected_languages)}")
            st.warning(f"No data available for the selected {', '.join(filter_desc)}.")
        elif selected_games:
        st.warning("No data available for the selected games.")
        else:
            st.warning("No data available.")
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


def render_question_correctness_chart(question_correctness_df: pd.DataFrame, selected_games: list) -> None:
    """Render stacked percent bar chart of Correct vs Incorrect per question for a selected game."""
    import altair as alt

    if question_correctness_df.empty:
        st.warning("No per-question correctness data available.")
        return

    st.markdown("### ✅ Question Correctness by Question Number")
    st.info("ℹ️ This section uses the global game, domain, and language filters from above.")

    # Use global game filter - function receives selected_games as parameter
    if not selected_games:
        st.warning("Please select at least one game in the global filters above.")
        return

    # Get available games from filtered data
    available_games = sorted(question_correctness_df['game_name'].dropna().unique())
    display_games = [g for g in selected_games if g in available_games]
    
    if not display_games:
        st.warning("No games available for the selected filters.")
        return

    # Show which game(s) are being displayed
    if len(display_games) == 1:
        selected_game = display_games[0]
        st.info(f"📊 Showing question correctness for: **{selected_game}**")
    else:
        # If multiple games selected, show the first one
        selected_game = display_games[0]
        st.info(f"📊 Showing question correctness for: **{selected_game}** (first of {len(display_games)} selected games)")

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


def render_video_viewership(video_viewership_df: pd.DataFrame) -> None:
    """Render video viewership data as a table with metrics."""
    if video_viewership_df.empty:
        st.warning("No video viewership data available.")
        return
    
    st.markdown("### 📹 Video Viewership Metrics")
    
    # Display the data as a formatted table
    display_df = video_viewership_df.copy()
    
    # Format numeric columns for better display
    if 'Average' in display_df.columns:
        display_df['Average'] = display_df['Average'].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
    if 'Min' in display_df.columns:
        display_df['Min'] = display_df['Min'].apply(lambda x: f"{int(x)}" if pd.notna(x) else "N/A")
    if 'Max' in display_df.columns:
        display_df['Max'] = display_df['Max'].apply(lambda x: f"{int(x)}" if pd.notna(x) else "N/A")
    
    # Format other numeric columns
    numeric_cols = ['Started', 'Questions', 'Rewards', 'Video Started']
    for col in numeric_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) else "N/A")
    
    # Display the table
    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True
    )
    
    # Add summary statistics
    if not video_viewership_df.empty:
        st.markdown("#### Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_started = video_viewership_df['Started'].sum() if 'Started' in video_viewership_df.columns else 0
            st.metric("Total Started", f"{int(total_started):,}")
        
        with col2:
            total_questions = video_viewership_df['Questions'].sum() if 'Questions' in video_viewership_df.columns else 0
            st.metric("Total Questions", f"{int(total_questions):,}")
        
        with col3:
            total_rewards = video_viewership_df['Rewards'].sum() if 'Rewards' in video_viewership_df.columns else 0
            st.metric("Total Rewards", f"{int(total_rewards):,}")
        
        with col4:
            total_video_started = video_viewership_df['Video Started'].sum() if 'Video Started' in video_viewership_df.columns else 0
            st.metric("Total Video Started", f"{int(total_video_started):,}")

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
         repeatability_df, score_distribution_df, poll_responses_df, question_correctness_df, video_viewership_df, metadata, processed_data_df, conversion_funnel_df, conversion_funnel_path) = load_processed_data()

    if summary_df.empty:
        st.warning("No data available.")
        return

    # Add global filters (applies to all sections except conversion funnel date range)
    st.markdown("### 🎮 Global Filters")
    
    # Domain filter - get unique domains from game_conversion_df or processed_data_df
    unique_domains = []
    if 'domain' in game_conversion_df.columns:
        unique_domains = sorted([d for d in game_conversion_df['domain'].dropna().unique() if d])
    elif 'domain' in processed_data_df.columns:
        unique_domains = sorted([d for d in processed_data_df['domain'].dropna().unique() if d])
    
    selected_domains = []
    if unique_domains:
        selected_domains = st.multiselect(
            "🌐 Select Domain(s) to filter by:",
            options=unique_domains,
            default=[],  # Empty by default - shows all domains
            help="Select one or more domains to filter all dashboard sections. Leave empty to show all domains. Domain is extracted from game_code (e.g., HY-01-CG-01 -> CG)."
        )
    
    # Game Name filter - get unique games from game_conversion_df
    unique_games = sorted(game_conversion_df['game_name'].unique())
    # If domain is selected, default to empty (show all games in domain)
    # Otherwise, default to first game
    if selected_domains and len(selected_domains) > 0:
        default_games = []  # When domain is selected, show all games in that domain by default
    else:
        default_games = [unique_games[0]] if unique_games else []  # Otherwise, default to first game
    selected_games = st.multiselect(
        "🎮 Select Game Names to filter by:",
        options=unique_games,
        default=default_games,
        help="Select one or more games to filter all dashboard sections. When domain is selected, leave empty to show all games in that domain."
    )
    
    # Language filter - get unique languages from game_conversion_df or processed_data_df (same pattern as domain filter)
    unique_languages = []
    if 'language' in game_conversion_df.columns:
        unique_languages = sorted([l for l in game_conversion_df['language'].dropna().unique() if l and str(l).strip() != ''])
    elif 'language' in processed_data_df.columns:
        unique_languages = sorted([l for l in processed_data_df['language'].dropna().unique() if l and str(l).strip() != ''])
    elif not conversion_funnel_df.empty and 'language' in conversion_funnel_df.columns:
        # Fallback to conversion_funnel_df if not found in main dataframes
        unique_languages = sorted([l for l in conversion_funnel_df['language'].dropna().unique() if l and str(l).strip() != ''])
    
    # Language filter - same pattern as domain filter
    selected_languages = []
    if unique_languages:
        selected_languages = st.multiselect(
            "🌍 Select Language(s) to filter by:",
            options=unique_languages,
            default=[],  # Empty by default - shows all languages
            help="Select one or more languages to filter all dashboard sections. Leave empty to show all languages."
        )
    else:
        # Show language filter as disabled if no languages found
        st.multiselect(
            "🌍 Select Language(s) to filter by:",
            options=[],
            default=[],
            disabled=True,
            help="No language data found. Please ensure game_conversion_numbers.csv or processed_data.csv has a 'language' column with language values."
        )
        # Show debug info to help diagnose the issue
        with st.expander("🔍 Debug: Language Filter Issue", expanded=False):
            st.write(f"**conversion_funnel_df:**")
            st.write(f"- Empty: {conversion_funnel_df.empty}")
            st.write(f"- Columns: {list(conversion_funnel_df.columns) if not conversion_funnel_df.empty else 'N/A'}")
            st.write(f"- Has 'language' column: {'language' in conversion_funnel_df.columns if not conversion_funnel_df.empty else 'N/A'}")
            # Show which file was loaded
            st.write(f"- Loaded from: {conversion_funnel_path if conversion_funnel_path else 'Unknown (fallback to processed_data_df)'}")
            st.write(f"**processed_data_df:**")
            st.write(f"- Empty: {processed_data_df.empty}")
            st.write(f"- Columns: {list(processed_data_df.columns) if not processed_data_df.empty else 'N/A'}")
            st.write(f"- Has 'language' column: {'language' in processed_data_df.columns if not processed_data_df.empty else 'N/A'}")
            st.write(f"**game_conversion_df:**")
            st.write(f"- Empty: {game_conversion_df.empty}")
            st.write(f"- Columns: {list(game_conversion_df.columns) if not game_conversion_df.empty else 'N/A'}")
            st.write(f"- Has 'language' column: {'language' in game_conversion_df.columns if not game_conversion_df.empty else 'N/A'}")
    
    # Show filter summary
    if not selected_games or len(selected_games) == len(unique_games):
        game_summary = "**All Games**"
        game_count = len(unique_games)
    else:
        game_summary = f"{', '.join(selected_games)}"
        game_count = len(selected_games)
    
    domain_summary = ""
    domain_count = 0
    if unique_domains:
        if not selected_domains or len(selected_domains) == len(unique_domains):
            domain_summary = "**All Domains**"
            domain_count = len(unique_domains)
        else:
            domain_summary = f"**{', '.join(selected_domains)}**"
            domain_count = len(selected_domains)
    
    language_summary = ""
    language_count = 0
    if unique_languages:
        if not selected_languages or len(selected_languages) == len(unique_languages):
            language_summary = "**All Languages**"
            language_count = len(unique_languages)
        else:
            language_summary = f"**{', '.join(selected_languages)}**"
            language_count = len(selected_languages)
    
    # Display filter summary
    if unique_domains or selected_games or unique_languages:
        filter_info = []
        if unique_domains:
            filter_info.append(f"🌐 Domain: {domain_summary} ({domain_count})")
        if selected_games:
            filter_info.append(f"🎮 Games: {game_summary} ({game_count})")
        if unique_languages:
            filter_info.append(f"🌍 Language: {language_summary} ({language_count})")
        if filter_info:
            st.info(" | ".join(filter_info))
    
    # Apply global filters (domain, game, and language) to all dataframes
    # Only consider a filter active if something is actually selected AND it's a subset of all options
    has_domain_filter = bool(selected_domains) and len(selected_domains) > 0 and len(selected_domains) < len(unique_domains) if unique_domains else False
    # Game filter: only active if games are explicitly selected AND it's a subset
    # If domain filter is active and no games are selected, don't apply game filter (show all games in domain)
    has_game_filter = bool(selected_games) and len(selected_games) > 0 and len(selected_games) < len(unique_games)
    # If domain filter is active and no games are selected, don't apply game filter
    if has_domain_filter and len(selected_games) == 0:
        has_game_filter = False  # Show all games within the selected domain
    # Language filter is active ONLY when languages are actually selected (not empty)
    # Unlike games/domains, we filter by language even if all languages are selected (to show explicit filtering)
    has_language_filter = bool(selected_languages) and len(selected_languages) > 0 if unique_languages else False
    
    # Helper function to filter dataframes by domain, game, and language
    def apply_global_filters(df, df_name=''):
        """Apply domain, game, and language filters to a dataframe"""
        filtered_df = df.copy()
        
        # Separate RM active users data (should always be preserved regardless of filters)
        rm_active_users_df = pd.DataFrame()
        if 'game_name' in filtered_df.columns and 'metric' in filtered_df.columns:
            rm_active_users_df = filtered_df[
                (filtered_df['game_name'] == 'All Games') & 
                (filtered_df['metric'] == 'rm_active_users')
            ].copy()
            # Remove RM active users from main dataframe before filtering
            filtered_df = filtered_df[
                ~((filtered_df['game_name'] == 'All Games') & 
                  (filtered_df['metric'] == 'rm_active_users'))
            ].copy()
        
        # For time series data, use "All" combinations similar to conversion funnel
        # Time series data uses 'game_code' column which contains domain codes (CG, LL, etc.) or 'All'
        # Use the same pattern as conversion funnel and parent poll responses
        if 'game_code' in filtered_df.columns and 'language' in filtered_df.columns and 'period_type' in filtered_df.columns:
            # Time series data has pre-calculated "All" combinations
            # Determine which combination to use based on global filters (same logic as conversion funnel)
            if not selected_domains and not selected_languages:
                # No filters - use overall totals (game_code='All', language='All')
                filtered_df = filtered_df[
                    (filtered_df['game_code'] == 'All') & (filtered_df['language'] == 'All')
                ]
            elif selected_domains and not selected_languages:
                # Domain filter only - use (game_code, language='All') combinations
                if len(selected_domains) == 1:
                    # Single domain - use exact combination
                    filtered_df = filtered_df[
                        (filtered_df['game_code'] == selected_domains[0]) & (filtered_df['language'] == 'All')
                    ]
                else:
                    # Multiple domains - aggregate (game_code, language='All') rows
                    filtered_df = filtered_df[
                        (filtered_df['game_code'].isin(selected_domains)) & (filtered_df['language'] == 'All')
                    ]
                    # Group by period, game, metric, event, and game_code to aggregate counts
                    group_cols = ['period_label', 'game_name', 'metric', 'event', 'period_type', 'game_code']
                    filtered_df = filtered_df.groupby(group_cols)['count'].sum().reset_index()
                    filtered_df['language'] = 'All'
            elif not selected_domains and selected_languages:
                # Language filter only - use (game_code='All', language) combinations
                if len(selected_languages) == 1:
                    # Single language - use exact combination
                    filtered_df = filtered_df[
                        (filtered_df['game_code'] == 'All') & (filtered_df['language'] == selected_languages[0])
                    ]
                else:
                    # Multiple languages - aggregate (game_code='All', language) rows
                    filtered_df = filtered_df[
                        (filtered_df['game_code'] == 'All') & (filtered_df['language'].isin(selected_languages))
                    ]
                    # Group by period, game, metric, event, and language to aggregate counts
                    group_cols = ['period_label', 'game_name', 'metric', 'event', 'period_type', 'language']
                    filtered_df = filtered_df.groupby(group_cols)['count'].sum().reset_index()
                    filtered_df['game_code'] = 'All'
            else:
                # Both domain and language filters - use (game_code, language) combinations
                if len(selected_domains) == 1 and len(selected_languages) == 1:
                    # Single domain and language - use exact combination
                    filtered_df = filtered_df[
                        (filtered_df['game_code'] == selected_domains[0]) & 
                        (filtered_df['language'] == selected_languages[0])
                    ]
                else:
                    # Multiple domains/languages - filter and aggregate
                    filtered_df = filtered_df[
                        (filtered_df['game_code'].isin(selected_domains)) & 
                        (filtered_df['language'].isin(selected_languages))
                    ]
                    # Group by period, game, metric, event, game_code, and language to aggregate counts
                    group_cols = ['period_label', 'game_name', 'metric', 'event', 'period_type', 'game_code', 'language']
                    filtered_df = filtered_df.groupby(group_cols)['count'].sum().reset_index()
        else:
            # For other dataframes, use the original filtering logic
            # Apply domain filter
            if has_domain_filter:
                if 'domain' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['domain'].isin(selected_domains)]
                elif 'game_code' in filtered_df.columns:
                    # Extract domain from game_code and filter
                    def extract_domain_from_game_code(game_code):
                        """Extract domain from game_code (e.g., HY-29-LL-06 -> LL)"""
                        if pd.isna(game_code) or not isinstance(game_code, str):
                            return None
                        parts = game_code.split('-')
                        if len(parts) >= 3:
                            return parts[2]
                        return None
                    filtered_df['domain'] = filtered_df['game_code'].apply(extract_domain_from_game_code)
                    filtered_df = filtered_df[filtered_df['domain'].isin(selected_domains)]
                elif 'game_name' in filtered_df.columns and 'domain' in game_conversion_df.columns:
                    # Filter by games in selected domains
                    games_in_domains = game_conversion_df[
                        game_conversion_df['domain'].isin(selected_domains)
                    ]['game_name'].unique()
                    filtered_df = filtered_df[filtered_df['game_name'].isin(games_in_domains)]
            
            # Apply game filter
            # If domain filter is active and no games are selected, show all games within the domain
            if has_game_filter:
                if 'game_name' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['game_name'].isin(selected_games)]
            
            # Apply language filter - same pattern as domain filter
            if has_language_filter:
                if 'language' in filtered_df.columns:
                    filtered_df = filtered_df[filtered_df['language'].isin(selected_languages)]
                elif 'game_name' in filtered_df.columns and 'language' in game_conversion_df.columns:
                    # Filter by games in selected languages
                    games_in_languages = game_conversion_df[
                        game_conversion_df['language'].isin(selected_languages)
                    ]['game_name'].unique()
                    filtered_df = filtered_df[filtered_df['game_name'].isin(games_in_languages)]
        
        # Combine filtered data with RM active users data
        if not rm_active_users_df.empty:
            filtered_df = pd.concat([filtered_df, rm_active_users_df], ignore_index=True)
        
        return filtered_df
    
    # Filter all dataframes with global filters (except repeatability and video viewership)
    filtered_time_series_df = apply_global_filters(time_series_df) if not time_series_df.empty else time_series_df
    filtered_score_distribution_df = apply_global_filters(score_distribution_df) if not score_distribution_df.empty else score_distribution_df
    filtered_question_correctness_df = apply_global_filters(question_correctness_df) if not question_correctness_df.empty else question_correctness_df
    filtered_poll_responses_df = apply_global_filters(poll_responses_df) if not poll_responses_df.empty else poll_responses_df
    # Repeatability and Video Viewership are NOT affected by global filters
    filtered_repeatability_df = repeatability_df  # Keep original, no global filters
    filtered_video_viewership_df = video_viewership_df  # Keep original, no global filters
    
    # Render conversion funnel section with date range filter
    st.markdown("---")
    st.markdown("## 🔄 Conversion Funnels")
    
    # Date range filter for conversion funnel (only for this section) - use conversion_funnel_df
    if 'date' in conversion_funnel_df.columns and not conversion_funnel_df.empty:
        min_date = conversion_funnel_df['date'].min()
        max_date = conversion_funnel_df['date'].max()
        date_range = st.date_input(
            "📅 Select Date Range for Conversion Funnel:",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Select a date range to filter the conversion funnel data."
        )
    else:
        date_range = None
    
    # Filter conversion_funnel_df by date range and global filters for conversion funnel
    has_date_filter = False
    if date_range and isinstance(date_range, tuple) and len(date_range) == 2 and date_range[0] and date_range[1]:
        if 'date' in conversion_funnel_df.columns and not conversion_funnel_df.empty:
            min_date = conversion_funnel_df['date'].min()
            max_date = conversion_funnel_df['date'].max()
            # Only consider it a filter if it's different from the full range
            if date_range[0] != min_date or date_range[1] != max_date:
                has_date_filter = True
    
    # Use pre-calculated combinations from summary_data.csv (similar to poll responses)
    # The data has all combinations: (All, All), (domain, All), (All, language), (domain, language)
    any_filter_applied = has_date_filter or has_game_filter or has_domain_filter or has_language_filter
    
    # Check if summary_df has domain and language columns (enhanced format)
    has_domain_in_summary = 'domain' in summary_df.columns
    has_language_in_summary = 'language' in summary_df.columns
    
    # Determine which combination to use from pre-calculated summary_data
    # Priority: Game filter > Domain/Language filters > No filters
    if not any_filter_applied:
        # No filters applied - use overall summary (domain='All', language='All')
        if has_domain_in_summary and has_language_in_summary:
            filtered_summary_df = summary_df[
                (summary_df['domain'] == 'All') & (summary_df['language'] == 'All')
            ].copy()
            if filtered_summary_df.empty:
                filtered_summary_df = summary_df.copy()
        else:
            filtered_summary_df = summary_df.copy()
    elif has_game_filter and not has_date_filter:
        # Game filter (without date filter) - use pre-calculated game_conversion_numbers.csv
        # This is much faster and more accurate than recalculating from raw data
        filtered_game_df = game_conversion_df.copy()
        
        # Apply game filter
        filtered_game_df = filtered_game_df[filtered_game_df['game_name'].isin(selected_games)]
        
        # Apply domain filter if set
        if has_domain_filter and 'domain' in filtered_game_df.columns:
            filtered_game_df = filtered_game_df[filtered_game_df['domain'].isin(selected_domains)]
        
        # Apply language filter if set
        if has_language_filter and 'language' in filtered_game_df.columns:
            # Normalize language values for comparison (strip whitespace, handle case)
            # Create a mask using normalized values but apply to original dataframe
            language_mask = filtered_game_df['language'].astype(str).str.strip().str.lower().isin(
                [str(lang).strip().lower() for lang in selected_languages]
            )
            filtered_game_df = filtered_game_df[language_mask]
        
        # Convert game_conversion_numbers format to summary format
        if not filtered_game_df.empty:
            funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
            summary_data = []
            for stage in funnel_stages:
                users_col = f'{stage}_users'
                visits_col = f'{stage}_visits'
                instances_col = f'{stage}_instances'
                
                users = filtered_game_df[users_col].sum() if users_col in filtered_game_df.columns else 0
                visits = filtered_game_df[visits_col].sum() if visits_col in filtered_game_df.columns else 0
                instances = filtered_game_df[instances_col].sum() if instances_col in filtered_game_df.columns else 0
                
                summary_data.append({
                    'Event': stage,
                    'Users': int(users),
                    'Visits': int(visits),
                    'Instances': int(instances)
                })
            filtered_summary_df = pd.DataFrame(summary_data)
        else:
            # No matching games - return empty summary
            all_events = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
            filtered_summary_df = pd.DataFrame({
                'Event': all_events,
                'Users': [0] * 8,
                'Visits': [0] * 8,
                'Instances': [0] * 8
            })
    elif has_date_filter:
        # Date filter requires recalculating from conversion_funnel_data
        if conversion_funnel_df.empty:
            # Fallback to summary_df with domain/language filters if available
            if has_domain_in_summary and has_language_in_summary:
                filtered_summary_df = _get_filtered_summary(summary_df, selected_domains, selected_languages, has_domain_filter, has_language_filter)
            else:
                filtered_summary_df = summary_df.copy()
        else:
            # Continue with existing logic for date filters (handled in else block below)
            # Don't set filtered_summary_df here - it will be set in the else block
            pass
    elif (has_domain_filter or has_language_filter) and has_domain_in_summary and has_language_in_summary:
        # Domain/language filters (without game filter) - use pre-calculated combinations from summary_data
        # This handles: domain only, language only, or both domain+language
        filtered_summary_df = _get_filtered_summary(summary_df, selected_domains, selected_languages, has_domain_filter, has_language_filter)
    elif conversion_funnel_df.empty:
        # Filters requested but no conversion funnel data available - use summary_df directly
        filtered_summary_df = summary_df.copy()
    
    # Handle date filters that require recalculating from conversion_funnel_data
    # (Game filters are now handled above using game_conversion_numbers.csv)
    if has_date_filter and not conversion_funnel_df.empty:
        # Filters are applied - recalculate summary from filtered conversion_funnel data
        filtered_conversion_funnel_data = conversion_funnel_df.copy()
        
        # Apply date filter
        if has_date_filter:
            start_date, end_date = date_range
            filtered_conversion_funnel_data = filtered_conversion_funnel_data[
                (filtered_conversion_funnel_data['date'] >= start_date) &
                (filtered_conversion_funnel_data['date'] <= end_date)
            ]
        
        # Apply domain filter
        if has_domain_filter:
            if 'domain' in filtered_conversion_funnel_data.columns:
                filtered_conversion_funnel_data = filtered_conversion_funnel_data[
                    filtered_conversion_funnel_data['domain'].isin(selected_domains)
                ]
            elif 'domain' in game_conversion_df.columns:
                # If domain is not in conversion_funnel but is in game_conversion_df, filter by games
                games_in_domains = game_conversion_df[
                    game_conversion_df['domain'].isin(selected_domains)
                ]['game_name'].unique()
                if 'game_name' in filtered_conversion_funnel_data.columns:
                    filtered_conversion_funnel_data = filtered_conversion_funnel_data[
                        filtered_conversion_funnel_data['game_name'].isin(games_in_domains)
                    ]
        
        # Apply game filter
        if has_game_filter:
            if 'game_name' in filtered_conversion_funnel_data.columns:
                filtered_conversion_funnel_data = filtered_conversion_funnel_data[
                    filtered_conversion_funnel_data['game_name'].isin(selected_games)
                ]
        
        # Apply language filter - ONLY if languages are actually selected (explicit check)
        # This prevents filtering when selected_languages is empty or None
        if has_language_filter and selected_languages is not None and len(selected_languages) > 0:
            if 'language' in filtered_conversion_funnel_data.columns:
                # Only filter if the column exists and we have valid selections
                filtered_conversion_funnel_data = filtered_conversion_funnel_data[
                    filtered_conversion_funnel_data['language'].isin(selected_languages)
                ]
        
        # Calculate funnel metrics from filtered conversion_funnel data
        # This ensures summary_data changes when any filter (including language) is applied
        # Debug: Check if event column exists
        if 'event' not in filtered_conversion_funnel_data.columns:
            # If event column doesn't exist, try to create it from 'name' column
            if 'name' in filtered_conversion_funnel_data.columns:
                # Import the parse function if available, or use a simple mapping
                def parse_event_from_name(name):
                    if pd.isna(name):
                        return None
                    name_str = str(name).lower()
                    if 'started' in name_str:
                        return 'started'
                    elif 'introduction_completed' in name_str and 'mid' not in name_str:
                        return 'introduction'
                    elif 'mid_introduction' in name_str:
                        return 'mid_introduction'
                    elif 'poll_completed' in name_str:
                        return 'parent_poll'
                    elif 'action_completed' in name_str:
                        return 'questions'
                    elif 'reward_completed' in name_str:
                        return 'rewards'
                    elif 'question_completed' in name_str:
                        return 'validation'
                    elif 'completed' in name_str:
                        return 'completed'
                    return None
                filtered_conversion_funnel_data['event'] = filtered_conversion_funnel_data['name'].apply(parse_event_from_name)
        
        if not filtered_conversion_funnel_data.empty and 'event' in filtered_conversion_funnel_data.columns:
            # Check if we have raw data columns - ALWAYS prefer raw data for accurate distinct counts
            has_idvisitor_converted = 'idvisitor_converted' in filtered_conversion_funnel_data.columns
            has_idvisitor = 'idvisitor' in filtered_conversion_funnel_data.columns
            has_idvisit = 'idvisit' in filtered_conversion_funnel_data.columns
            has_idlink_va = 'idlink_va' in filtered_conversion_funnel_data.columns
            has_raw_data = has_idvisitor_converted or has_idvisitor
            
            # Check if it's the new aggregated format (has instances, visits, users columns)
            has_aggregated = 'instances' in filtered_conversion_funnel_data.columns and 'visits' in filtered_conversion_funnel_data.columns and 'users' in filtered_conversion_funnel_data.columns
            
            # If we have raw data, use it (even if aggregated columns also exist)
            if has_raw_data:
                # Use raw data format - calculate distinct counts
                funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
                filtered_summary_data = []
                for stage in funnel_stages:
                    stage_data = filtered_conversion_funnel_data[filtered_conversion_funnel_data['event'] == stage]
                    if not stage_data.empty:
                        # Use distinct counts from raw data
                        if has_idvisitor_converted:
                            users_count = stage_data['idvisitor_converted'].nunique()
                        elif has_idvisitor:
                            users_count = stage_data['idvisitor'].nunique()
                        else:
                            users_count = 0
                        
                        if has_idvisit:
                            visits_count = stage_data['idvisit'].nunique()
                        else:
                            visits_count = 0
                        
                        if has_idlink_va:
                            instances_count = len(stage_data)
                        else:
                            instances_count = 0
                        
                        filtered_summary_data.append({
                            'Event': stage,
                            'Users': users_count,
                            'Visits': visits_count,
                            'Instances': instances_count
                        })
                    else:
                        filtered_summary_data.append({
                            'Event': stage,
                            'Users': 0,
                            'Visits': 0,
                            'Instances': 0
                        })
                filtered_summary_df = pd.DataFrame(filtered_summary_data)
            elif has_aggregated:
                # Only aggregated format available
                # Instances: sum is always correct
                # Visits: sum is acceptable (may have slight inflation but works)
                # Users: sum causes inflation - try to load raw data, otherwise use aggregated with note
                raw_conversion_funnel = None
                root_path = "conversion_funnel.csv"
                data_path = os.path.join(DATA_DIR, "conversion_funnel.csv")
                
                # Try to load raw data for accurate Users calculation
                for path in [root_path, data_path]:
                    if os.path.exists(path):
                        try:
                            sample = pd.read_csv(path, nrows=5, low_memory=False)
                            is_raw = 'idlink_va' in sample.columns or 'idvisitor' in sample.columns or 'idvisitor_converted' in sample.columns
                            if is_raw:
                                raw_conversion_funnel = pd.read_csv(path, low_memory=False)
                                # Apply same filters to raw data
                                if 'date' in raw_conversion_funnel.columns or 'server_time' in raw_conversion_funnel.columns:
                                    if 'date' not in raw_conversion_funnel.columns and 'server_time' in raw_conversion_funnel.columns:
                                        raw_conversion_funnel['date'] = pd.to_datetime(raw_conversion_funnel['server_time']).dt.date
                                    if has_date_filter:
                                        start_date, end_date = date_range
                                        raw_conversion_funnel = raw_conversion_funnel[
                                            (raw_conversion_funnel['date'] >= start_date) &
                                            (raw_conversion_funnel['date'] <= end_date)
                                        ]
                                if has_domain_filter and 'domain' in raw_conversion_funnel.columns:
                                    raw_conversion_funnel = raw_conversion_funnel[raw_conversion_funnel['domain'].isin(selected_domains)]
                                if has_game_filter and 'game_name' in raw_conversion_funnel.columns:
                                    raw_conversion_funnel = raw_conversion_funnel[raw_conversion_funnel['game_name'].isin(selected_games)]
                                if has_language_filter and 'language' in raw_conversion_funnel.columns:
                                    raw_conversion_funnel = raw_conversion_funnel[raw_conversion_funnel['language'].isin(selected_languages)]
                                
                                # Ensure idvisitor_converted exists
                                if 'idvisitor' in raw_conversion_funnel.columns and 'idvisitor_converted' not in raw_conversion_funnel.columns:
                                    raw_conversion_funnel['idvisitor_converted'] = raw_conversion_funnel['idvisitor']
                                
                                # Create event column if needed
                                if 'event' not in raw_conversion_funnel.columns and 'name' in raw_conversion_funnel.columns:
                                    def parse_event_from_name(name):
                                        if pd.isna(name):
                                            return None
                                        name_str = str(name).lower()
                                        if 'started' in name_str:
                                            return 'started'
                                        elif 'introduction_completed' in name_str and 'mid' not in name_str:
                                            return 'introduction'
                                        elif 'mid_introduction' in name_str:
                                            return 'mid_introduction'
                                        elif 'poll_completed' in name_str:
                                            return 'parent_poll'
                                        elif 'action_completed' in name_str:
                                            return 'questions'
                                        elif 'reward_completed' in name_str:
                                            return 'rewards'
                                        elif 'question_completed' in name_str:
                                            return 'validation'
                                        elif 'completed' in name_str:
                                            return 'completed'
                                        return None
                                    raw_conversion_funnel['event'] = raw_conversion_funnel['name'].apply(parse_event_from_name)
                                break
                        except Exception:
                            continue
                
                # Calculate metrics from aggregated data
                funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
                filtered_summary_data = []
                for stage in funnel_stages:
                    stage_data = filtered_conversion_funnel_data[filtered_conversion_funnel_data['event'] == stage]
                    if not stage_data.empty:
                        # Instances: sum is always correct
                        instances_count = stage_data['instances'].sum()
                        
                        # Visits: sum is acceptable (may have slight inflation but works for filtered data)
                        visits_count = stage_data['visits'].sum()
                        
                        # Users: use raw data if available for accurate distinct counts, otherwise use aggregated sum
                        if raw_conversion_funnel is not None and not raw_conversion_funnel.empty and 'event' in raw_conversion_funnel.columns:
                            # Use raw data for accurate Users count (distinct count)
                            raw_stage_data = raw_conversion_funnel[raw_conversion_funnel['event'] == stage]
                            if not raw_stage_data.empty:
                                users_count = raw_stage_data['idvisitor_converted'].nunique() if 'idvisitor_converted' in raw_stage_data.columns else stage_data['users'].sum()
                            else:
                                users_count = 0
                        else:
                            # No raw data available - use sum from aggregated data
                            # Note: This may have some inflation as same user can appear in multiple rows
                            # but it's the best approximation we can get from aggregated data
                            users_count = stage_data['users'].sum()
                        
                        filtered_summary_data.append({
                            'Event': stage,
                            'Users': users_count,
                            'Visits': visits_count,
                            'Instances': instances_count
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
                funnel_stages = ['started', 'introduction', 'questions', 'mid_introduction', 'validation', 'parent_poll', 'rewards', 'completed']
                filtered_summary_data = []
                for stage in funnel_stages:
                    stage_data = filtered_conversion_funnel_data[filtered_conversion_funnel_data['event'] == stage]
                    filtered_summary_data.append({
                        'Event': stage,
                        'Users': stage_data['idvisitor_converted'].nunique() if 'idvisitor_converted' in stage_data.columns else 0,
                        'Visits': stage_data['idvisit'].nunique() if 'idvisit' in stage_data.columns else 0,
                        'Instances': len(stage_data)
                    })
                filtered_summary_df = pd.DataFrame(filtered_summary_data)
        else:
            # Fallback to summary_df if filtered_conversion_funnel_data is empty or missing event column
            if has_domain_in_summary and has_language_in_summary:
                filtered_summary_df = _get_filtered_summary(summary_df, selected_domains, selected_languages, has_domain_filter, has_language_filter)
            else:
                filtered_summary_df = summary_df.copy()
    
    # Ensure filtered_summary_df is always set (fallback if not set above)
    if 'filtered_summary_df' not in locals():
        if has_domain_in_summary and has_language_in_summary:
            filtered_summary_df = _get_filtered_summary(summary_df, selected_domains, selected_languages, has_domain_filter, has_language_filter)
        else:
            filtered_summary_df = summary_df.copy()
    
    # Render conversion funnel with date and game filters applied
    render_modern_dashboard(filtered_summary_df, filtered_summary_df)
    
    # Add Time Series Analysis - right after conversion funnels
    st.markdown("---")
    st.markdown("## 📈 Time-Series Analysis")
    
    if not filtered_time_series_df.empty:
        render_time_series_analysis(filtered_time_series_df, game_conversion_df, selected_games)
    else:
        st.warning("No time series data available.")
    
    # Add Score Distribution Analysis
    st.markdown("---")
    st.markdown("## 🎯 Score Distribution Analysis")
    
    if not filtered_score_distribution_df.empty:
        render_score_distribution_chart(filtered_score_distribution_df, selected_games)
    else:
        st.warning("No score distribution data available.")
    
    # Add Question Correctness by Question Number Analysis
    st.markdown("---")
    st.markdown("## ✅ Question Correctness by Question Number")
    
    if not filtered_question_correctness_df.empty:
        render_question_correctness_chart(filtered_question_correctness_df, selected_games)
    else:
        st.warning("No question correctness data available. Please run preprocess_data.py to generate the data.")
    
    # Add Parent Poll Responses Analysis
    st.markdown("---")
    st.markdown("## 📊 Parent Poll Responses Analysis")
    
    if not filtered_poll_responses_df.empty:
        render_parent_poll_responses(filtered_poll_responses_df, game_conversion_df, selected_games, selected_domains, selected_languages, has_domain_filter, has_language_filter)
    else:
        st.warning("No parent poll responses data available.")
    
    # Add Repeatability Analysis
    st.markdown("---")
    st.markdown("## 🎮 Game Repeatability Analysis")
    
    if not filtered_repeatability_df.empty:
        render_repeatability_analysis(filtered_repeatability_df)
    else:
        st.warning("No repeatability data available.")
    
    # Add Video Viewership Analysis
    st.markdown("---")
    st.markdown("## 📹 Video Viewership")
    
    if not filtered_video_viewership_df.empty:
        render_video_viewership(filtered_video_viewership_df)
    else:
        st.warning("No video viewership data available.")


# Streamlit automatically runs the script, so call main() directly
main()