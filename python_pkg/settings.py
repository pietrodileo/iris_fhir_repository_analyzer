"""
Configuration settings for the IRIS Patient Search application.
Handles environment variables and application constants.
"""

import os
import dotenv
import json
from dataclasses import dataclass

@dataclass
class Config:
    """Application configuration class."""
    
    def __init__(self, json_config=None, prompt_template=None, env_file=None):
        """Initialize configuration by loading environment variables."""
        # Load environment variables from .env file
        dotenv.load_dotenv(env_file)
        
        # Database configuration
        self.IRIS_HOST = os.getenv("IRIS_HOST")
        self.IRIS_PORT = os.getenv("IRIS_PORT")
        self.IRIS_NAMESPACE = os.getenv("IRIS_NAMESPACE")
        self.IRIS_USER = os.getenv("IRIS_USER")
        self.IRIS_PASSWORD = os.getenv("IRIS_PASSWORD")
        
        # Model configuration
        self.TRANSFORMER_MODEL = os.getenv("TRANSFORMER_MODEL")
        self.OLLAMA_API_URL = os.getenv("OLLAMA_API_URL")
        
        # Maximum number of medical records of a category to pass to the LLM
        self.MAX_RECORDS = int(os.getenv("MAX_RECORDS")) if os.getenv("MAX_RECORDS") else None
        
        # import json
        if json_config:
            with open(json_config, 'r', encoding="utf-8") as f:
                data = json.load(f)

            # Application constants
            self.MEDICAL_TABLES = data.get("MEDICAL_TABLES")
            
            self.AVAILABLE_MODELS = data.get("AVAILABLE_MODELS")
        
        # import txt file
        if prompt_template:
            with open(prompt_template, 'r') as f:
                self.DEFAULT_PROMPT_TEMPLATE = f.read()
        
    def validate(self):
        """Validate that all required configuration values are present."""
        required_vars = [
            'IRIS_HOST', 'IRIS_PORT', 'IRIS_NAMESPACE', 
            'IRIS_USER', 'IRIS_PASSWORD', 'TRANSFORMER_MODEL', 
            'OLLAMA_API_URL'
        ]
        
        missing_vars = [var for var in required_vars if not getattr(self, var)]
        
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        return True

# Global config instance
config = Config()