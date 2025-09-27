"""
Patient service for handling patient data operations.
"""

import yaml
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class PatientService:
    """Service class for patient data operations."""
    
    def prepare_patient_data_for_llm(self, selected_row, tables, max_records):
        """
        Prepare patient data in YAML format for LLM processing.
        
        Parameters
        ----------
        selected_row : pd.DataFrame
            Selected patient row data
        tables : dict
            Dictionary of medical tables data
            
        Returns
        -------
        str
            Formatted patient data string for LLM input
        """
        try:
            # Extract patient demographics
            row_dict = selected_row.to_dict()
            row_idx = int(selected_row.index.to_list()[0])
            
            patient_demographics = {
                "full_name": row_dict["full_name"][row_idx],
                "gender": row_dict["gender"][row_idx],
                "birthdate": self.safe_timestamp(row_dict["birthdate"][row_idx], date_only=True),
                "age": int(row_dict["age"][row_idx]) if not pd.isna(row_dict["age"][row_idx]) else None,
                "deceased": row_dict["deceased"][row_idx],
                "deceased_datetime": row_dict["deceased_datetime"][row_idx],
                "country": row_dict["country"][row_idx],
                "state": row_dict["state"][row_idx],
                "city": row_dict["city"][row_idx],
            }
            
            patient_demographics_yaml = yaml.dump(patient_demographics)
            # logger.info(f"Demographics: {patient_demographics_yaml}")
            
            # Process medical tables
            medical_data = self._process_medical_tables(tables, max_records)
            
            # Format final data string
            data_string = f"""
            ### Patient Data
            Demographics:
            {patient_demographics_yaml}

            Allergies:
            {medical_data['allergies']}

            Immunizations:
            {medical_data['immunizations']}

            Conditions:
            {medical_data['conditions']}

            Procedures:
            {medical_data['procedures']}

            Care Plans:
            {medical_data['careplans']}

            Observation Trends:
            {medical_data['observations']}
            """
           
            # logger.info(f"data_string: {data_string}")
            
            return data_string
            
        except Exception as e:
            logger.error(f"Error preparing patient data for LLM: {e}")
            raise
    
    def _process_medical_tables(self, tables, max_records=20):
        """Process medical tables into YAML format."""
        medical_data = {
            'allergies': [],
            'immunizations': [],
            'conditions': [],
            'observations': [],
            'procedures': [],
            'careplans': []
        }
        
        table_mapping = {
            'AllergyIntolerance': 'allergies',
            'Immunization': 'immunizations', 
            'Condition': 'conditions',
            'Observation': 'observations',
            'Procedures': 'procedures',
            'CarePlan': 'careplans'
        }
        
        for table_name, data_key in table_mapping.items():
            if table_name in tables and not tables[table_name].empty:
                # reverse rows so most recent are first
                df = tables[table_name].iloc[::-1]
                # Limit number of records to create a more balanced prompt and focused context on the last records
                records = df.head(max_records).to_dict("records")
                normalized = [self.normalize_record(r) for r in records]
                # remove patient_id column
                for record in normalized:
                    record.pop("patient_id", None)
                medical_data[data_key] = yaml.dump(normalized, allow_unicode=True)
            else:
                medical_data[data_key] = "No records found"
        
        return medical_data
    
    import pandas as pd

    def normalize_record(self,record):
        """Convert pandas/numpy types into plain Python."""
        out = {}
        for k, v in record.items():
            if pd.isna(v):
                out[k] = None
            elif isinstance(v, pd.Timestamp):
                out[k] = v.isoformat()  # or v.strftime("%Y-%m-%d") if you want only date
            elif hasattr(v, "item"):  # numpy types
                out[k] = v.item()
            else:
                out[k] = v
        return out

    def safe_timestamp(self, value, date_only=False):
        if pd.isna(value):
            return None
        if isinstance(value, pd.Timestamp):
            return value.strftime("%Y-%m-%d") if date_only else value.isoformat()
        return value
