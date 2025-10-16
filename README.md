# IRIS Patient Search via FHIR repository analysis

This proof of concept (PoC) demonstrates how InterSystems IRIS can integrate with external languages via the Python SDK (IRIS Native) to create and analyze a FHIR repository. Finally, the data is visualized with Streamlit, featuring hybrid semantic search to locate patients and a local LLM model to generate patient histories from extracted records.

## 🎯 Features

This project is a Streamlit-based web application for hybrid search and comprehensive analysis of patient medical records using InterSystems IRIS database with vector search capabilities and AI-powered patient history generation.

- **Create and analyze a FHIR repository**: Create a repository composed by FHIR messages containing clinical information and analyze it to retrieve structured medical data
- **Semantic Patient Search**: Natural language search through patient descriptions using vector embeddings
- **Advanced Filtering**: Hybrid search can be performed by filtering the patients by gender, deceased status, age range
- **Comprehensive Patient Profiles**: View detailed medical records across multiple categories:
  - 🤧 Allergies & Intolerances
  - 💉 Immunizations
  - 📊 Observations & Lab Results
  - 🩺 Medical Conditions
  - ⚕️ Procedures
  - 📋 Care Plans
- **AI-Powered History Generation**: Generate comprehensive patient summaries using various local LLM models
- **Interactive Data Exploration**: Navigate patient records with an intuitive interface

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker

### Installation

1. **Clone the repository and navigate to the project directory**
   ```bash
    git clone https://github.com/pietrodileo/iris_fhir_repository_analyzer.git
   ```

2. **Create and activate a virtual environment**
   I like to us `uv` package manager but you can use whatever

   ```bash
   uv venv
   .\.venv\Scripts\activate
   ```

3. **Install dependencies:**

   ```bash
   uv pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   Edit `python_pkg/config/.env` with your configuration:

   ```bash
   # IRIS Database Configuration
   IRIS_HOST=your_iris_host
   IRIS_PORT=your_iris_port  
   IRIS_NAMESPACE=your_namespace
   IRIS_USER=your_username
   IRIS_PASSWORD=your_password
   
   # Model Configuration
   TRANSFORMER_MODEL=your_transformer_model
   OLLAMA_API_URL=your_ollama_api_url
   MAX_RECORDS=max_number_of_record_the_llm_will_analyze_per_category
   ```

   Feel free to use the default configuration:

   ```bash
   IRIS_HOST=127.0.0.1
   IRIS_PORT=9091
   IRIS_NAMESPACE=USER
   IRIS_USER=_SYSTEM
   IRIS_PASSWORD=SYS
   TRANSFORMER_MODEL=pritamdeka/S-PubMedBert-MS-MARCO
   OLLAMA_API_URL=http://localhost:11424/api/chat
   MAX_RECORDS=20   
   ```

5. **Run Docker Compose**
   This will pull two images:
   - `ollama/ollama:latest`
   - `intersystems/iris-community:latest-cd`

   Ollama image will install three models by default:
   - `llama3.2:1b`
   - `gemma2:2b`
   - `gemma3:1b`
   You can configurate them by the `ollama_entrypoint.sh` file.

  Build the images and run the containers:
   ```bash
   docker compose up -d --build
   ```

   Please ensure that Docker has completed all the downloads before approaching the next step.

6. **Create database by importing FHIR examples:**
   Once Docker is running, you can import FHIR examples to create a repository. FHIR bundles in the `fhir_examples` directory have been synthetically generated and can be found at [this repository](https://github.com/smart-on-fhir/generated-sample-data). 

   To create the database run the following command, it will take a few minutes since at first start it will pull the sentence transformer. Then, More than 1000 FHIR bundles will be imported.

   ```bash
   uv run create_db.py
   ```

   These are the expected logs:

   ```bash
   2025-09-27 09:28:56,830 - INFO - IRIS connection established successfully
   2025-09-27 09:28:56,837 - INFO - Use pytorch device_name: cpu
   2025-09-27 09:28:56,837 - INFO - Load pretrained SentenceTransformer: pritamdeka/S-PubMedBert-MS-MARCO
   2025-09-27 09:29:00,265 - INFO - Transformer model pritamdeka/S-PubMedBert-MS-MARCO loaded successfully
   2025-09-27 09:29:00,266 - INFO - Connections established successfully
   Table FHIRrepository dropped successfully.
   Table CarePlan dropped successfully.
   Table Procedures dropped successfully.
   Table Condition dropped successfully.
   Table Observation dropped successfully.
   Table Immunization dropped successfully.
   Table AllergyIntolerance dropped successfully.
   Table Patient dropped successfully.
   Table FHIRrepository created successfully.
   Index patient_id_idx created successfully on FHIRrepository(patient_id).
   Table Patient created successfully.
   Index patient_id_idx created successfully on Patient(patient_id).
   Index age_idx created successfully on Patient(age).
   Index gender_idx created successfully on Patient(gender).
   Created HNSW index description_vector_idx on Patient(description_vector)
   Table SQLUser.AllergyIntolerance created successfully.
   Index patient_id_idx created successfully on AllergyIntolerance(patient_id).
   Table SQLUser.Immunization created successfully.
   Index patient_id_idx created successfully on Immunization(patient_id).
   Table SQLUser.Observation created successfully.
   Index patient_id_idx created successfully on Observation(patient_id).
   Table SQLUser.Condition created successfully.
   Index patient_id_idx created successfully on Condition(patient_id).
   Table SQLUser.Procedures created successfully.
   Index patient_id_idx created successfully on Procedures(patient_id).
   Table SQLUser.CarePlan created successfully.
   Index patient_id_idx created successfully on CarePlan(patient_id).
   Index patient_id_idx already exists
   Index patient_id_idx already exists
   Index age_idx already exists
   Index gender_idx already exists
   Table AllergyIntolerance already exists
   Index patient_id_idx already exists
   Table Immunization already exists
   Index patient_id_idx already exists
   Table Observation already exists
   Index patient_id_idx already exists
   Table Condition already exists
   Index patient_id_idx already exists
   Table Procedures already exists
   Index patient_id_idx already exists
   Table CarePlan already exists
   Index patient_id_idx already exists
   2025-09-27 09:29:02,977 - INFO - Tables created successfully
   2025-09-27 09:29:24,613 - INFO - FHIR data imported successfully
   ```

7. **Run the application:**

   ```bash
   uv run streamlit run main.py
   ```

## 📊 Database Schema

The application will create the following tables in your IRIS database:

- `SQLUser.FHIRrepository` - Contains all the raw FHIR messages associated to a patient id

![FHIRrepository](pic/FHIRrepository_created.png "FHIRrepository")

- `SQLUser.Patient` - Patient demographics and descriptions with vector embeddings
- `SQLUser.AllergyIntolerance` - Allergy and intolerance records
- `SQLUser.Immunization` - Vaccination records  
- `SQLUser.Observation` - Lab results and observations
- `SQLUser.Condition` - Medical conditions and diagnoses
- `SQLUser.Procedures` - Medical procedures 
- `SQLUser.CarePlan` - Treatment and care plans

![Patient](pic/Patient_table_details.png "Patient")

## 🔧 Usage

1. **Search Patients**: Enter natural language descriptions (e.g., "diabetes with cardiovascular issues"). Use sidebar filters to narrow results by demographics. Click on "Search" button to perform an hybrid search
![Search](pic/UI_pat_search_example.png "Search")

2. **Select Patient**: Choose one patient from search results to view detailed profile
![Profile](pic/UI_patient_profile1.png "Profile")

3. **Explore Records**: Browse medical records through organized tabs
![Profile2](pic/UI_patient_profile2.png "Profile2")

4. **Generate History**: Edit the prompt if needed, then use ollama to create comprehensive patient summaries. Performance may vary upon your workstation since ollama run entirily locally on Docker container.
![Prompt](pic/UI_pat_history_prompt.png "Prompt")

5. **Generate History**: See the result
![PatHistory](pic/UI_pat_history_results.png "PatHistory")

Generated patient history varies upon the selected model and prompt. In the `output_examples` folder you can an example of three history generated for the same patient but with different models.

### Example of Usage

The following video shows an example of usage of the application:

https://github.com/user-attachments/assets/74f328e6-b597-4f21-b7cb-0b8a6d2d2d72


