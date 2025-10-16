from .utils.fhir_analyzer import FHIRAnalyzer

class FHIRExtactor:
    def __init__(self, transformer, iris_conn):
        """
        Initialize the FHIRExtactor with the transformer and IRIS connection.

        Args:
            transformer (Transformer): Transformer object
            iris_conn (IRIStool): IRIS connection object

        Returns:
            None
        """
        self.fhir_analyzer = FHIRAnalyzer()
        self.transformer = transformer
        self.iris_conn = iris_conn

    def extract_data(self):
        """
        Extract data from FHIR repository and insert it into IRIS tables.

        The function queries the FHIR repository table in IRIS to retrieve all
        patient IDs and their corresponding FHIR bundles. It then extracts the
        demographic data from each bundle using the FHIR analyzer, computes
        a vector embedding for each patient's demographic information using
        the transformer, and inserts the data into the corresponding IRIS tables.

        Args:
            None

        Raises:
            Exception: If an error occurs while extracting or inserting the data.
        """
        try:    
            table_name = "FHIRrepository"
            if not self.iris_conn.table_exists(table_name=table_name, table_schema="SQLUser"):
                print(f"Table {table_name} does not exist")
                raise Exception(f"Table {table_name} does not exist")

            df = self.iris_conn.fetch(f"SELECT patient_id, fhir_bundle FROM {table_name}")
            for bundle, patient_id in zip(df["fhir_bundle"], df["patient_id"]):
                pat_info_str, pat_data = self.fhir_analyzer.analyze_fhir(bundle)
                embedding = self.transformer.create_vector(pat_info_str)
                resources_to_skip = ['Encounter','DiagnosticReport']
                if pat_data:
                    for resource in pat_data.keys():
                        data = pat_data[resource]
                        resource_type = data["resource_type"]
                        if resource_type in resources_to_skip:
                            continue
                        table_name = resource_type
                        # check if table exists
                        if not self.iris_conn.table_exists(table_name=table_name, table_schema="SQLUser"):
                            print(f"Table {resource_type} does not exist")
                            raise Exception(f"Table {resource_type} does not exist")
                        # insert data
                        if resource_type == "Patient":
                            input_data = {
                                "patient_id": patient_id,
                                "description": pat_info_str,
                                "description_vector": str(embedding['vector']),
                                "full_name": data["full_name"],
                                "gender": data["gender"],
                                "age": int(data["current_age"]),
                                "birthdate": data["birth_date"],
                                "phone": data["phone"],
                                "email": data["email"],
                                "address": data["address"],
                                "state": data["state"],
                                "city": data["city"],
                                "country": data["country"],
                                "social_security_number": data["ssn_id"],
                                "medical_record_number": data["mrn_id"],
                                "driver_license": data["driver_license"],
                                "passport_number": data["passport_number"],
                                "deceased": True if data['deceased_datetime'] else False,
                                "deceased_datetime": data["deceased_datetime"],
                            }
                            # insert data into the specific table
                            self.iris_conn.insert_row(table_name=table_name, values=input_data)
                            pat_row_id = self.iris_conn.get_row_id(table_name="Patient", column_name="patient_id", value=patient_id)
                        else:
                            for element in data["elements"]:
                                # copy all the data but the resource type from the keys
                                input_data = {
                                    "patient_id": pat_row_id,
                                    **element
                                }
                                input_data.pop("resource_type")
                                self.iris_conn.insert_row(table_name=table_name, values=input_data)
                else:
                    print(f"No demographic data found for patient {patient_id}")
        except Exception as e:
            print(e)