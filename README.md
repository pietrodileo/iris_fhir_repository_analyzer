# IRIS Patient Search Application

A Streamlit-based web application for semantic search and comprehensive analysis of patient medical records using InterSystems IRIS database with vector search capabilities and AI-powered patient history generation.

## 🎯 Features

- **Semantic Patient Search**: Natural language search through patient descriptions using vector embeddings
- **Advanced Filtering**: Filter patients by gender, deceased status, age range, and birthdate
- **Comprehensive Patient Profiles**: View detailed medical records across multiple categories:
  - 🤧 Allergies & Intolerances
  - 💉 Immunizations
  - 📊 Observations & Lab Results
  - 🩺 Medical Conditions
  - ⚕️ Procedures
  - 📋 Care Plans
- **AI-Powered History Generation**: Generate comprehensive patient summaries using various LLM models
- **Interactive Data Exploration**: Browse and analyze patient data with intuitive tabbed interface

## 🏗️ Architecture

The application follows a modular architecture with clear separation of concerns:

```
├── main.py                  # Application entry point
├── config/                  # Configuration management
├── utils/                   # Database connections & query builders
├── ui/                      # User interface components
├── services/                # Business logic services
└── python_pkg/             # External dependencies
```

## 🚀 Quick Start

### Prerequisites

- Python 3.8+
- InterSystems IRIS database with vector search capabilities
- Ollama API server
- Required Python packages (see requirements.txt)

### Installation

1. **Clone and navigate to the project directory**
2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment variables:**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` with your configuration:
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
   ```

4. **Run the application:**
   ```bash
   streamlit run main.py
   ```

## 📊 Database Schema

The application expects the following tables in your IRIS database:
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

## 🤖 AI Models

Supported Ollama models for history generation:
- Gemma 3 (270M, 1B, 4B parameters)
- Gemma 2 (2B parameters)
- Llama 3.2 (1B parameters)
- TinyLlama (1.1B parameters)

## 📝 Contest Deployment

For contest environments where `.env` files might not be suitable:

1. **Environment Variables**: Set variables directly in your deployment environment
2. **Configuration Override**: Modify `config/settings.py` to use hardcoded values if needed
3. **Dependencies**: Ensure all packages in `requirements.txt` are installed
4. **Database Access**: Verify IRIS database connectivity and proper table setup

## 🔒 Security Notes

- Never commit `.env` files with real credentials
- Use environment variables for sensitive configuration
- Ensure database connections are properly secured
- Validate all user inputs before database queries

## 🛠️ Development

The modular structure makes it easy to:
- Add new UI components in `ui/`
- Extend business logic in `services/`
- Add new database operations in `utils/`
- Modify configuration in `config/`

## 📋 Dependencies

- **Streamlit**: Web application framework
- **Pandas**: Data manipulation and analysis
- **PyYAML**: YAML parsing for data formatting
- **python-dotenv**: Environment variable management
- **Custom packages**: IRIS connector, transformer, and Ollama client

## 🎯 Use Cases

- **Clinical Research**: Search patients by complex medical criteria
- **Case Studies**: Generate comprehensive patient summaries
- **Medical Education**: Explore patient data patterns and trends  
- **Care Coordination**: Review patient histories across multiple encounters