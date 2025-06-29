#NB: non usare la classe ResourceManager, o potrebbero verificarsi dei loop di import

import vertexai

from vertexai.generative_models import GenerativeModel, GenerationConfig
from logger_utils import logger


PROJECT_ID = "gruppo-4-456912"
LOCATION = "europe-west1"
MODEL_NAME = "gemini-2.0-flash-001"


# Initialize Vertex AI
def init():
    try:
        logger.info(f"Initializing Vertex AI with project '{PROJECT_ID}' and location '{LOCATION}'...")
        vertexai.init(project=PROJECT_ID, location=LOCATION)

    except Exception as e:
        logger.error(f"[CRR][vertexai_utils][init] Failed to initialize Vertex AI: {e}")
        exit()


# Load the generative model
def get_model() -> GenerativeModel:
    try:
        logger.info(f"Loading model '{MODEL_NAME}'")
        model = GenerativeModel(MODEL_NAME)

        return model
    
    except Exception as e:
        logger.error(f"[CRR][vertexai_utils][init] Failed to load Vertex AI model: {e}")
        exit()


# Configuration (low temperature = more deterministic)
def get_generation_config() -> GenerationConfig:
    return GenerationConfig(
        temperature=0.2,
        top_p=1,
        top_k=1,
        max_output_tokens=512
    )
