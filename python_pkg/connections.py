"""
Connection utilities for IRIS database, transformer, and LLM services.
"""

import streamlit as st
import logging
from python_pkg.utils.iris_connector import IRIS_connection
from python_pkg.utils.transformer import Transformer
from python_pkg.utils.ollama_request import ollama_request

logger = logging.getLogger(__name__)

def init_iris_connection(config) -> IRIS_connection:
    """Initialize IRIS database connection."""
    try:
        iris_conn = IRIS_connection(
            host=config.IRIS_HOST,
            port=int(config.IRIS_PORT),
            namespace=config.IRIS_NAMESPACE,
            username=config.IRIS_USER,
            password=config.IRIS_PASSWORD
        )
        logger.info("IRIS connection established successfully")
        return iris_conn
    except Exception as e:
        logger.error(f"Failed to establish IRIS connection: {e}")
        raise

def init_transformer(config) -> Transformer:
    """Initialize transformer model."""
    try:
        transformer = Transformer(config.TRANSFORMER_MODEL)
        logger.info(f"Transformer model {config.TRANSFORMER_MODEL} loaded successfully")
        return transformer
    except Exception as e:
        logger.error(f"Failed to load transformer model: {e}")
        raise

def init_ollama_client(config):
    """Initialize Ollama API client."""
    try:
        llm = ollama_request(config.OLLAMA_API_URL)
        logger.info("Ollama client initialized successfully")
        return llm
    except Exception as e:
        logger.error(f"Failed to initialize Ollama client: {e}")
        raise

@st.cache_resource
def initialize_connections(config):
    """Initialize and cache all connections."""
    config.validate()  # Validate configuration first
    
    iris_conn = init_iris_connection(config)
    transformer = init_transformer(config)
    llm = init_ollama_client(config)
    
    return iris_conn, transformer, llm