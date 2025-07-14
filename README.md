LLM4SOC – False Positive Triage Assistant for IDS Alert Analysis

Sistema di classificazione alert da IDS (AIT-ADS) come minacce reali o falsi positivi, con spiegazione leggibile tramite LLM (Gemini). Include server FastAPI, servizi Worker e Merge Handler containerizzati su Cloud Run, dashboard React e infrastruttura GCP gestita con Terraform.

## Struttura
.
├── Alerts/                # Dataset CSV/JSON
├── cloud_function/        # Merge handler (Cloud Functions)
├── cloud_run_worker/      # Worker per analisi LLM (Cloud Run)
├── fast_api_server/       # API FastAPI
├── llm4soc/               # Parsing e classificazione alert
└── README.md

## Tech stack
* Python 3.11 • FastAPI • Terraform
* GCP: Cloud Storage, Compute Engine, Cloud Build, Cloud Run, Vertex AI, Eventarc, IAM, Cloud Tasks
* Dataset: AIT-ADS (IDS logs)
* LLM: Gemini (via Vertex AI)

## Avvio locale API
_ Aprire 2 terminali:
  source llm4soc-env/bin/activate --> per ciascuno
  cd fast_api_server --> per ciascuno
  uvicorn app:app --host 0.0.0.0 --port 8000 --> in uno solo
  uvicorn benchmark:app --host 0.0.0.0 --port 8001 --> in uno solo

## Deployment GCP
cd terraform/
terraform init && terraform apply

## Avvio Dashboard
cd llm4soc-dashboard
npm start

## Autori
Federico Mancini, Samuele Mazziotti
UNIBO – SRS, a.a. 2024/2025
