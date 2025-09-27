"""
Search UI components for patient search functionality.
"""

import streamlit as st
import logging

logger = logging.getLogger(__name__)

def build_hybrid_query(embedding_str, num_results, gender, deceased, age_range):
    """
    Build a hybrid query for IRIS using vector search and filters.

    Parameters
    ----------
    embedding_str : str
        The string representation of the vector to search against
    num_results : int
        The number of results to return
    gender : str
        The gender to filter by
    deceased : str
        The deceased status to filter by
    age_range : tuple
        The age range to filter by

    Returns
    -------
    query : str
        The SQL query string
    parameters : list
        The parameters to pass to the query
    """
    parameters = [num_results]
    query = f"""
    SELECT TOP ?
        patient_row_id, description,
        patient_id, full_name, gender, age, birthdate, deceased,
        social_security_number, medical_record_number, driver_license, passport_number,
        address, phone, email, state, city, country, deceased_datetime
    """    
    if embedding_str != "" and embedding_str is not None:
        logger.info(f"Inserisco embedding_str")
        query += f"""
        ,VECTOR_DOT_PRODUCT(description_vector,TO_VECTOR(?,float)) as similarity
        """
        parameters.append(embedding_str)
    else:
        query += f"""
        ,0 as similarity
        """
    query += f"""
    FROM SQLUser.Patient
    WHERE 1=1
    """
    
    # Add gender filter
    if gender != "Any":
        query += f" AND gender = ?"
        parameters.append(gender)
    
    # Add deceased status filter
    if deceased == "Alive":
        query += " AND (deceased = 0 OR deceased IS NULL)"
    elif deceased == "Deceased":
        query += " AND deceased = 1"
    
    # Add age range filter  
    if age_range != (0, 120):
        min_year = age_range[0]
        max_year = age_range[1]
        query += f" AND age BETWEEN ? AND ?"
        parameters.append(min_year)        
        parameters.append(max_year)
    
    # Add vector similarity ordering
    if embedding_str != "" and embedding_str is not None:
        query += " ORDER BY VECTOR_DOT_PRODUCT(description_vector,TO_VECTOR(?,float)) DESC "
        parameters.append(embedding_str)
        
    return query, parameters

def build_patient_medical_query(table_name, patient_row_id):
    """
    Build query to fetch patient medical data from specific table.
    
    Parameters
    ----------
    table_name : str
        Name of the medical table to query
    patient_row_id : int
        Patient row ID to filter by
        
    Returns
    -------
    str
        SQL query string
    """
    return f"SELECT * FROM SQLUser.{table_name} WHERE patient_id = {patient_row_id}"

def render_search_section(filters):
    """Render the search input section and handle search execution."""
    
    search_query = st.text_input(
        "Search patients (vector search)",
        placeholder="e.g., diabetes with cardiovascular issues",
        help="Use natural language to describe the patients you're looking for"
    )
    
    num_results = st.number_input(
        "Number of results", 
        min_value=1, 
        max_value=100, 
        step=1, 
        value=15, 
        help="Number of patients to retrieve"
    )

    if st.button("🔍 Search"):
        if not search_query.strip():
            st.warning("Research will be performed without a search query.")
        execute_search(search_query, num_results, filters)

def execute_search(search_query, num_results, filters):
    """Execute the patient search with given parameters."""
    with st.spinner("Searching patients..."):
        try:
            # Get connections from session state
            iris_conn = st.session_state.iris_conn
            transformer = st.session_state.transformer

            # Create embedding for search query     
            if search_query.strip() == "":
                embedding_str = None
            else:
                embedding = transformer.create_vector(search_query)
                embedding_str = str(embedding['vector'])
            
            # Build and execute query
            query, parameters = build_hybrid_query(
                embedding_str, 
                num_results, 
                filters['gender'], 
                filters['deceased'], 
                filters['age_range']
            )
            
            results = iris_conn.query(query, parameters)
            
            if not results.empty:
                st.session_state.search_results = results
                st.success(f"Found {len(results)} patients")
            else:
                st.warning("No patients found.")
                
        except Exception as e:
            logger.error(f"Search failed: {e}")
            st.error(f"Search failed: {e}")