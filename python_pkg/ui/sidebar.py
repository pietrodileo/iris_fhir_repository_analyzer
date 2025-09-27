"""
Sidebar UI components for filters and navigation.
"""

import streamlit as st

def render_sidebar_filters():
    """Render sidebar filters and return filter values."""
    st.sidebar.header("🔎 Filters")

    filters = {
        'gender': st.sidebar.selectbox("Gender", ["Any", "Male", "Female"]),
        'deceased': st.sidebar.selectbox("Deceased", ["Any", "Alive", "Deceased"]),
        'age_range': st.sidebar.slider("Age Range", 0, 120, (0, 120))
    }
    
    return filters