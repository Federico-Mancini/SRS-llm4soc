import os, vertexai
from vertexai.generative_models import GenerativeModel, GenerationConfig


# Variabili d'ambiente non condivise su GCS (per motivi di sicurezza)
# (TODO: trovare alternative per memorizzare in maniera sicura)
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT") or "gruppo-4-456912"
LOCATION = os.environ.get("GOOGLE_CLOUD_REGION") or "europe-west1"
MODEL_NAME = "gemini-2.0-flash-001"


# Initialize Vertex AI
def init():
    try:
        print(f"Initializing Vertex AI with project '{PROJECT_ID}' and location '{LOCATION}'...")
        vertexai.init(project=PROJECT_ID, location=LOCATION)
        print("Vertex AI initialized successfully.")

    except Exception as e:
        print(f"Error initializing Vertex AI. Please ensure PROJECT_ID '{PROJECT_ID}' and LOCATION '{LOCATION}' are correct, and your service account has 'Vertex AI User' permissions.")
        print(f"Details: {e}")
        exit()


# Load the generative model
def get_model() -> GenerativeModel:
    try:
        model = GenerativeModel(MODEL_NAME)
        print(f"Model '{MODEL_NAME}' loaded.")

        return model
    
    except Exception as e:
        print(f"Error loading model '{MODEL_NAME}'. It might not be available in region '{LOCATION}' or your project doesn't have access.")
        print(f"Details: {e}")
        exit()


# Configuration (low temperature = more deterministic)
def get_generation_config() -> GenerationConfig:
    return GenerationConfig(
        temperature=0.2,
        top_p=1,
        top_k=1,
        max_output_tokens=512
    )
