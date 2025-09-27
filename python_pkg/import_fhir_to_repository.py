import os
from .utils.fhir_analyzer import FHIRAnalyzer
import json

class FHIRImporter:
    def __init__(self,iris_conn, folder_path="fhir_examples", repository_name="FHIRrepository"):
        """
        Initialize the FHIR importer with the IRIS connection, folder path, and repository name.

        Args:
            iris_conn (IRIS_connection): IRIS connection object
            folder_path (str, optional): Path to the folder containing FHIR JSON files. Defaults to "fhir_examples".
            repository_name (str, optional): Name of the repository in IRIS. Defaults to "FHIRrepository".

        Raises:
            FileNotFoundError: If the folder path does not exist.
            Exception: If the repository name does not exist in IRIS.
        """
        self.conn = iris_conn   
        # verify if folder exists 
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"Folder {folder_path} exists")
        else:  
            self.folder_path = folder_path
        # verify if table exists
        if not self.conn.table_exists(repository_name, table_schema="SQLUser"):
            raise Exception(f"Table {repository_name} does not exist")
        else:
            self.repository_name = repository_name
        
    def import_fhir(self):
        """
        Import FHIR data from the specified folder into the IRIS repository.

        The function walks through the folder and all its subfolders, opens each
        JSON file, extracts the patient identifiers, and inserts the data
        into the IRIS repository.

        Args:
            None

        Raises:
            Exception: If an error occurs while importing the FHIR data.
        """
        try: 
            # extract fhir data
            fhir_analyzer = FHIRAnalyzer(self.folder_path)

            for root, dirs, files in os.walk(fhir_analyzer.folder_path):
                for file in files:
                    if file.endswith(".json"):
                        file_path = os.path.join(root, file)
                        with open(file_path, 'r', encoding='utf-8') as f:
                            # load json
                            data = json.load(f)
                        # extract patient id
                        patient_uuid = fhir_analyzer.extract_patient_identifiers(data)
                        input_data = {
                            "patient_id": patient_uuid,
                            "fhir_bundle": json.dumps(data)  # serialize dict to JSON string
                        }
                        self.conn.insert(table_name=self.repository_name, **input_data)
        except Exception as e:
            raise Exception(f"Error importing FHIR data: {e}")