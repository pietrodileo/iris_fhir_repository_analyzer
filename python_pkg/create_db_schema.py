from .utils.iris_connector import IRIS_connection

class IrisFHIRSchema:
    def __init__(self, iris_conn):
        """
        Initialize the IrisFHIRSchema object with an IRIS connection.

        Args:
            iris_conn (IRIS_connection): The IRIS connection object to use for creating the FHIR schema.
            
        """
        self.conn = iris_conn

    def init_schema(self):
        """
        Drop all FHIR tables (if exist) in the correct order,
        then recreate them with proper foreign keys and indices.
        """
        # Drop in reverse dependency order
        tables = [
            "FHIRrepository",
            "CarePlan",
            # "DiagnosticReport",
            "Procedures",
            "Condition",
            "Observation",
            "Immunization",
            "AllergyIntolerance",
            "Patient"
        ]
        for t in tables:
            self.conn.drop_table(t)

        # Now recreate schema
        self.create_tables()

    def create_tables(self):
        # Generic SQL schema
        """
        Create all tables required for the FHIR repository schema.

        The method will first drop all tables (if exist) in the correct order,
        then recreate them with proper foreign keys and indices.

        The tables created are:
            - FHIRrepository: stores patient_id and corresponding FHIR bundle data
            - Patient: stores patient details
            - AllergyIntolerance: stores allergy intolerance data with foreign key to Patient
            - Immunization: stores immunization data with foreign key to Patient
            - Observation: stores observation data with foreign key to Patient
            - Condition: stores condition data with foreign key to Patient
            - Procedures: stores procedures data with foreign key to Patient
            - CarePlan: stores care plan data with foreign key to Patient

        The method will also create indices on the patient_id column of the
        respective tables.

        The method will also create an HNSW index on the description_vector
        column of the Patient table for efficient querying of patient descriptions.

        :return: None
        :raises Exception: If there is an error creating the tables.
        """
        table_schema = "SQLUser"
        
        # --- FHIR repository table ---
        table_name = "FHIRrepository"
        if not self.conn.table_exists(table_name,table_schema):
            self.conn.create_table(
                table_name=table_name,
                columns={
                    "patient_id": "VARCHAR(200)",
                    "fhir_bundle": "CLOB"
                }
            )
        self.conn.quick_create_index(table_name=table_name, column_name="patient_id")

        # --- Patient table ---
        table_name = "Patient"
        if not self.conn.table_exists(table_name):
            self.conn.create_table(
                table_name=table_name,
                columns={
                    "patient_row_id": "INT AUTO_INCREMENT PRIMARY KEY",
                    "description": "CLOB",
                    "description_vector": "VECTOR(FLOAT, 768)",
                    "patient_id": "VARCHAR(200)",
                    "full_name": "VARCHAR(200)",
                    "gender": "VARCHAR(30)",
                    "age": "INTEGER",
                    "birthdate": "TIMESTAMP",
                    "phone": "VARCHAR(30)",
                    "email": "VARCHAR(30)",
                    "address": "VARCHAR(200)",
                    "country": "VARCHAR(100)",
                    "state": "VARCHAR(100)",
                    "city": "VARCHAR(100)",
                    "social_security_number": "VARCHAR(100)",
                    "medical_record_number": "VARCHAR(100)",
                    "driver_license": "VARCHAR(100)",
                    "passport_number": "VARCHAR(100)",
                    "deceased": "BIT",
                    "deceased_datetime": "TIMESTAMP"
                }
            )
        self.conn.quick_create_index(table_name=table_name, column_name="patient_id")
        self.conn.quick_create_index(table_name=table_name, column_name="age")
        self.conn.quick_create_index(table_name=table_name, column_name="gender")

        index_name = "description_vector_idx"
        if not self.conn.index_exists(table_name=table_name, index_name=index_name):
            self.conn.create_hnsw_index(
                index_name=index_name,
                table_name=table_name,
                column_name="description_vector",
                distance="Cosine"
            )

        # --- Other tables with foreign key to Patient ---
        fk = ["FOREIGN KEY(patient_id) REFERENCES Patient(patient_row_id)"]

        self._create_child_table(
            "AllergyIntolerance",
            {
                "patient_id": "INT",
                "type": "VARCHAR(200)",
                "category": "VARCHAR(100)",
                "criticality": "VARCHAR(200)",
                "code": "VARCHAR(200)",
                "assertedDate": "TIMESTAMP",
                "clinical_status": "VARCHAR(100)",
                "verification_status": "VARCHAR(100)"
            },
            constraints=fk
        )

        self._create_child_table(
            "Immunization",
            {
                "patient_id": "INT",
                "vaccine_code": "VARCHAR(200)",
                "imm_date": "TIMESTAMP",
                "status": "VARCHAR(100)"
            },
            constraints=fk
        )

        self._create_child_table(
            "Observation",
            {
                "patient_id": "INT",
                "code": "VARCHAR(200)",
                "obs_date": "TIMESTAMP",
                "value": "VARCHAR(100)",
                "unit": "VARCHAR(100)"
            },
            constraints=fk
        )

        self._create_child_table(
            "Condition",
            {
                "patient_id": "INT",
                "code": "VARCHAR(200)",
                "onset": "TIMESTAMP",
                "assertedDate": "TIMESTAMP",
                "clinical_status": "VARCHAR(100)",
                "verification_status": "VARCHAR(100)"
            },
            constraints=fk
        )

        self._create_child_table(
            "Procedures",
            {
                "patient_id": "INT",
                "code": "VARCHAR(200)",
                "proc_date": "TIMESTAMP"
            },
            constraints=fk
        )

        # self._create_child_table(
        #     "DiagnosticReport",
        #     {
        #         "patient_id": "INT",
        #         "code": "VARCHAR(200)",
        #         "issued": "VARCHAR(100)"
        #     },
        #     constraints=fk
        # )

        self._create_child_table(
            "CarePlan",
            {
                "patient_id": "INT",
                "category": "VARCHAR(200)",
                "cp_start": "TIMESTAMP",
                "cp_end": "TIMESTAMP",
                "status": "VARCHAR(100)",
                "activities": "VARCHAR(500)"
            },
            constraints=fk
        )

    def _create_child_table(self, table_name: str, columns: dict, constraints=None):
        """
        Create a child table with a foreign key to Patient.

        Args:
            table_name (str): The name of the table to create.
            columns (dict): A dictionary of column names to their respective data types.
            constraints (list[str] | None, optional): A list of constraints to apply to the table. Defaults to None.

        Returns:
            None
        """
        if not self.conn.table_exists(table_name, table_schema="SQLUser"):
            self.conn.create_table(
                table_name=table_name,
                table_schema="SQLUser",
                columns=columns,
                constraints=constraints
            )
        else:
            print(f"Table {table_name} already exists")

        self.conn.quick_create_index(table_name=table_name, column_name="patient_id")

if __name__ == "__main__":
    iris_conn = IRIS_connection(host="127.0.0.1", port=9091, namespace='FHIROLLAMA', username='_SYSTEM', password='SYS')
    iris_fhir_schema = IrisFHIRSchema(iris_conn)
    iris_fhir_schema.init_schema()
    iris_fhir_schema.create_tables()