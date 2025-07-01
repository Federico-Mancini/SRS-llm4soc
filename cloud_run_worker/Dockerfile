# Immagine base
FROM python:3.11-slim

# Imposta working directory remota
WORKDIR /app

# Copia file locali in '/app'
COPY . /app

# Installa dipendenze
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Esponi la porta per Cloud Run
EXPOSE 8080

# Comando di avvio (FastAPI via Uvicorn)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
