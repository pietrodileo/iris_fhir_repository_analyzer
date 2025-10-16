from python_pkg.utils.iristool import IRIStool
from python_pkg.create_db_schema import IrisFHIRSchema
from python_pkg.import_fhir_to_repository import FHIRImporter
from python_pkg.extract_data_from_fhir import FHIRExtactor
from python_pkg.utils.transformer import Transformer
import logging
import dotenv
import os 
# import functools

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_file = "python_pkg/config/.env"
dotenv.load_dotenv(env_file)
IRIS_HOST = os.getenv("IRIS_HOST")
IRIS_PORT = os.getenv("IRIS_PORT")
IRIS_NAMESPACE = os.getenv("IRIS_NAMESPACE")
IRIS_USER = os.getenv("IRIS_USER")
IRIS_PASSWORD = os.getenv("IRIS_PASSWORD")
TRANSFORMER_MODEL = os.getenv("TRANSFORMER_MODEL")

def init_transformer() -> Transformer:
    """Initialize and cache the transformer model."""
    try:
        transformer = Transformer(TRANSFORMER_MODEL)
        logger.info(f"Transformer model {TRANSFORMER_MODEL} loaded successfully")
        return transformer
    except Exception as e:
        logger.error(f"Failed to load transformer model: {e}")
        raise

if __name__ == "__main__":
    with IRIStool(
            host=IRIS_HOST,
            port=int(IRIS_PORT),
            namespace=IRIS_NAMESPACE,
            username=IRIS_USER,
            password=IRIS_PASSWORD
        ) as iris_conn:
        
        logger.info("IRIS connection established successfully")
        try:
            transformer = init_transformer()
        except Exception as e:
            logger.error(f"Failed to initialize IRIS connection or transformer: {e}")
            exit(1)
            
        logger.info("Connections established successfully")

        iris_fhir_schema = IrisFHIRSchema(iris_conn)
        iris_fhir_schema.init_schema()
        iris_fhir_schema.create_tables()

        logger.info("Tables created successfully")

        fhir_importer = FHIRImporter(iris_conn, folder_path="fhir_examples", repository_name="FHIRrepository")
        fhir_importer.import_fhir()
        
        logger.info("FHIR data imported successfully")
        
        fhir_extractor = FHIRExtactor(transformer, iris_conn)
        fhir_extractor.extract_data()
        
        logger.info("Data extracted and inserted successfully")