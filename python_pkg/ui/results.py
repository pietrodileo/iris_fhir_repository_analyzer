"""
Results UI components for displaying search results.
"""

import streamlit as st
import pandas as pd

def render_results_section():
    """Render search results table with patient selection."""
    if st.session_state.search_results is None:
        return
    
    st.markdown("---")
    st.subheader("📊 Search Results")
    
    df = st.session_state.search_results.copy()
    
    # Define columns to display
    cols_to_show = [
        "similarity", "description", "patient_id", "full_name", "gender",
        "age", "birthdate", "address", "phone", "email", "deceased",
        "deceased_datetime", "social_security_number",
        "city", "state", "country",
        "driver_license", "passport_number"
    ]
    
    # Keep only existing columns
    cols_to_show = [c for c in cols_to_show if c in df.columns]
    df_display = df[cols_to_show].reset_index(drop=True).copy()
    
    # Add checkbox column for selection
    df_display.insert(0, "Select", pd.Series([False] * len(df_display), dtype="bool"))
    
    edited = st.data_editor(
        df_display,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select",
                help="Check to select this patient (only 1 allowed at a time)",
                default=False,
            )
        },
        width='stretch'
    )
    
    handle_patient_selection(edited)

def handle_patient_selection(edited_df):
    """Handle patient selection logic - ensure only one patient is selected."""
    selected_rows = edited_df[edited_df["Select"] == True]
    
    if len(selected_rows) == 1:
        # Single selection - valid
        st.session_state.selected_patient = selected_rows.iloc[0]["patient_id"]
        st.session_state.selected_patient_name = selected_rows.iloc[0]["full_name"]
        st.session_state.selected_row = selected_rows
    elif len(selected_rows) > 1:
        # Multiple selections - invalid
        st.error("❌ You can select only one patient at a time.")
        clear_selection()
    else:
        # No selection
        clear_selection()

def clear_selection():
    """Clear patient selection from session state."""
    st.session_state.selected_patient = None
    st.session_state.selected_patient_name = None
    st.session_state.selected_row = {}
    st.session_state.selected_row_id = None