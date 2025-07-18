#NB: non usare la classe ResourceManager, o potrebbero verificarsi dei loop di import

import vertexai

from vertexai.generative_models import GenerativeModel, GenerationConfig
from utils.logger_utils import logger


PROJECT_ID = "gruppo-4-456912"
LOCATION = "europe-west1"
MODEL_NAME = "gemini-2.0-flash-001"


# F01 - Initialize Vertex AI
def init():
    try:
        logger.info(f"[vertex|F01]\t-> Initializing Vertex AI with project '{PROJECT_ID}' and location '{LOCATION}'...")
        vertexai.init(project=PROJECT_ID, location=LOCATION)

    except Exception as e:
        msg = f"[vertex|F01]\t-> Failed to initialize Vertex AI: {str(e)}"
        logger.error(msg)
        raise RuntimeError(msg)


# F02 - Load the generative model
def get_model() -> GenerativeModel:
    try:
        logger.info(f"[vertex|F02]\t-> Loading model '{MODEL_NAME}'")
        model = GenerativeModel(MODEL_NAME)

        return model
    
    except Exception as e:
        msg = f"[vertex|F02]\t-> Failed to load Vertex AI model: {str(e)}"
        logger.error(msg)
        raise RuntimeError(msg)


# F03 - Configuration (low temperature = more deterministic)
def get_generation_config() -> GenerationConfig:
    return GenerationConfig(
        temperature=0.2,
        top_p=1,
        top_k=1,
        max_output_tokens=512
    )
