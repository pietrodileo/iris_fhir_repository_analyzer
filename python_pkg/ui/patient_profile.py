"""
Patient profile UI components for displaying detailed patient information.
"""

import streamlit as st
import logging
from .patient_history import PatientService

logger = logging.getLogger(__name__)

def render_patient_profile(config):
    """Render patient profile section when a patient is selected."""
    if not st.session_state.selected_patient:
        return
        
    st.markdown("---")
    st.subheader(f"👤 Patient Profile - {st.session_state.selected_patient_name} - ID: {st.session_state.selected_patient}")
    st.write("Show patient details and generate a patient history")
    
    # Get patient row ID
    patient_id = st.session_state.selected_patient
    iris_conn = st.session_state.iris_conn
    
    try:
        pat_row_id = iris_conn.get_row_id(table_name="Patient", column_name="patient_id", value=patient_id)
        st.session_state.selected_row_id = pat_row_id
        logger.info(f"Selected row ID: {st.session_state.selected_row_id}")
        
        if pat_row_id is not None:
            render_medical_records_tabs(pat_row_id,config)
            render_records_summary(config)
            render_history_generation_section(config)
        else:
            st.error("Could not retrieve patient row ID")
            
    except Exception as e:
        logger.error(f"Error loading patient data: {str(e)}")
        st.error(f"Error loading patient data: {str(e)}")

def render_medical_records_tabs(pat_row_id,config):
    """Render tabs for different medical record types."""
    medical_tables = config.MEDICAL_TABLES
    
    # Create tabs for each medical table
    tabs = st.tabs([f"{info['icon']} {table_name}" for table_name, info in medical_tables.items()])
    
    for idx, (table_name, table_info) in enumerate(medical_tables.items()):
        with tabs[idx]:
            st.markdown(f"#### {table_info['icon']} {table_name} Records")
            
            try:
                iris_conn = st.session_state.iris_conn
                query = f"SELECT * FROM SQLUser.{table_name} WHERE patient_id = {pat_row_id}"
                table_data = iris_conn.fetch(query)
                
                if not table_data.empty:
                    st.write(f"Found {len(table_data)} records in {table_name}")
                    st.dataframe(table_data, width='stretch')
                else:
                    st.info(f"No {table_name.lower()} records found for this patient")
                    
            except Exception as e:
                st.error(f"Error loading {table_name} data: {str(e)}")

def render_records_summary(config):
    """Render patient records summary with metrics."""
    st.markdown("---")
    st.markdown("#### 📈 Patient Records Summary")
    
    # Collect data for summary
    summary_data = {}
    st.session_state.tables = {}
    pat_row_id = st.session_state.selected_row_id
    iris_conn = st.session_state.iris_conn
    
    for table_name in config.MEDICAL_TABLES.keys():
        try:
            query = f"SELECT * FROM SQLUser.{table_name} WHERE patient_id = {pat_row_id}"
            table_data = iris_conn.fetch(query)
            st.session_state.tables[table_name] = table_data
            summary_data[table_name] = len(table_data) if not table_data.empty else 0
        except Exception as e:
            logger.error(f"Error loading {table_name} data: {str(e)}")
            summary_data[table_name] = 0
    
    # Display summary metrics in columns
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("🤧 Allergies", summary_data.get("AllergyIntolerance", 0))
        st.metric("💉 Immunizations", summary_data.get("Immunization", 0))
    
    with col2:
        st.metric("📊 Observations", summary_data.get("Observation", 0))
        st.metric("🩺 Conditions", summary_data.get("Condition", 0))
    
    with col3:
        st.metric("⚕️ Procedures", summary_data.get("Procedures", 0))
        st.metric("📋 Care Plans", summary_data.get("CarePlan", 0))

def render_history_generation_section(config):
    """Render the patient history generation section."""
    st.markdown("---")
    st.markdown("### 🤖 Patient History Generation")
    st.write("Generate a comprehensive patient history summary using AI analysis of all medical data")
    
    patient_id = st.session_state.selected_patient
    
    # Get total records count
    total_records = sum(
        len(st.session_state.tables.get(table, []))
        for table in config.MEDICAL_TABLES.keys()
    )
    
    # Custom prompt with template
    prompt_template = config.DEFAULT_PROMPT_TEMPLATE.format(
        patient_id=patient_id,
        total_records=total_records
    )
    
    custom_prompt = st.text_area(
        "Insert your custom prompt here", 
        prompt_template, 
        height=500
    )
    
    # Model selector
    selected_model = st.selectbox(
        "Select Model:", 
        options=config.AVAILABLE_MODELS, 
        index=0
    )
    
    if st.button("Create Patient History"):
        logger.info(f"Creating patient history for patient ID: {patient_id}")
        generate_patient_history(custom_prompt, selected_model, config.MAX_RECORDS)

def generate_patient_history(custom_prompt, selected_model, max_records):
    """Generate patient history using LLM service."""
    try:
        patient_service = PatientService()
        
        # Get patient data
        patient_data = patient_service.prepare_patient_data_for_llm(
            st.session_state.selected_row,
            st.session_state.tables,
            max_records
        )
        
        # Generate history
        llm_input = f"{custom_prompt}\n\n{patient_data}"
        
        logger.info(f"LLM input: {llm_input}")
        
        with st.spinner("Asking LLM..."):
            st.markdown("#### Generated Patient History")
            st.write_stream(
                st.session_state.llm.get_response(
                    content=llm_input, 
                    model=selected_model
                )
            )
            
    except Exception as e:
        # logger.error(f"LLM call failed: {e}")
        st.error(f"LLM call failed: {e}")