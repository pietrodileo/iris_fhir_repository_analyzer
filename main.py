"""
IRIS Patient Search Application - Main Entry Point
A Streamlit application for semantic search and analysis of patient data using IRIS database.
"""

import streamlit as st
import logging
from python_pkg.settings import Config
from python_pkg.connections import initialize_connections
from python_pkg.ui.sidebar import render_sidebar_filters
from python_pkg.ui.search import render_search_section
from python_pkg.ui.results import render_results_section
from python_pkg.ui.patient_profile import render_patient_profile

env_file = "python_pkg/config/.env"
settings_file = "python_pkg/config/settings.json"
prompt_template = "python_pkg/config/prompt_template.txt"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_app():
    """Initialize the Streamlit application."""
    st.set_page_config(
        page_title="IRIS Patient Search",
        page_icon="🏥",
        layout="wide",
    )

def initialize_session_state():
    """Initialize all session state variables."""
    defaults = {
        "selected_patient": None,
        "selected_patient_name": "",
        "selected_row": {},
        "tables": {},
        "generated_history": None,
        "selected_row_id": None,
        "search_results": None
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value

def main():
    """Main application entry point."""
    # Initialize app configuration
    initialize_app()
    initialize_session_state()

    # Initialize connections
    logger.info("Initializing connections...")
    logger.info(f"Settings: {settings_file}, Prompt Template: {prompt_template}, Env File: {env_file}")
    config = Config(json_config=settings_file, prompt_template=prompt_template, env_file=env_file)
    try:
        iris_conn, transformer, llm = initialize_connections(config)
        st.sidebar.success("✅ Connected to IRIS")
    except Exception as e:
        st.sidebar.error(f"❌ Connection failed: {e}")
        st.stop()
    
    # Store connections in session state for access across components
    st.session_state.iris_conn = iris_conn
    st.session_state.transformer = transformer
    st.session_state.llm = llm
    
    # Render main UI components
    st.title("🏥 IRIS Patient Search")
    
    # Render sidebar filters
    filters = render_sidebar_filters()
    
    # Render search section
    render_search_section(filters)
    
    # Render results section
    render_results_section()
    
    # Render patient profile section
    render_patient_profile(config)

if __name__ == "__main__":
    main()