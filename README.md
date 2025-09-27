# IRIS Patient Search enabled through FHIR repository analysis

This is a POC to demonstrate how InterSystems IRIS can be used to interact with an external language via the Python SDK (IRIS Native) to create and analyze a FHIR repository. Finally, the data is visualized using Streamlit, featuring hybrid search to locate the patient and a local LLM model to generate a patient history based on the extracted data.

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
- **AI-Powered History Generation**: Generate comprehensive patient summaries using various LLM models
- **Interactive Data Exploration**: Browse and analyze patient data with intuitive tabbed interface

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- Docker

### Installation

1. **Clone and navigate to the project directory**

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

   ```bash
   docker compose up -d --build
   ```

   Please ensure that Docker has completed all the downloads before approaching the next step. 

6. **Create database by importing FHIR examples:**
   Once Docker is running you can import FHIR examples to create a repository. FHIR messages in the `fhir_examples` directory have been synthetically generated and can be found at [this repository](https://github.com/smart-on-fhir/generated-sample-data). For this project a group of 1000 FHIR messages will be used.

   To create the database run the following command, it will take approximately 5 minutes or less:

   ```bash
   uv run create_db.py
   ```

7. **Run the application:**

   ```bash
   uv run streamlit run main.py
   ```

## 📊 Database Schema

The application will create the following tables in your IRIS database:

- `SQLUser.Patient` - Patient demographics and descriptions with vector embeddings
- `SQLUser.AllergyIntolerance` - Allergy and intolerance records
- `SQLUser.Immunization` - Vaccination records  
- `SQLUser.Observation` - Lab results and vital signs
- `SQLUser.Condition` - Medical conditions and diagnoses
- `SQLUser.Procedures` - Medical procedures and treatments
- `SQLUser.CarePlan` - Treatment and care plans

## 🔧 Usage

1. **Search Patients**: Enter natural language descriptions (e.g., "diabetes with cardiovascular issues")
2. **Apply Filters**: Use sidebar filters to narrow results by demographics
3. **Select Patient**: Choose one patient from search results to view detailed profile
4. **Explore Records**: Browse medical records through organized tabs
5. **Generate History**: Use AI to create comprehensive patient summaries

## 🎯 Use Cases

- **Clinical Research**: Search patients by complex medical criteria
- **Case Studies**: Generate comprehensive patient summaries
- **Medical Education**: Explore patient data patterns and trends  
- **Care Coordination**: Review patient histories across multiple encounters