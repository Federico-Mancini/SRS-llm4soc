#NB: non usare la classe ResourceManager, o potrebbero verificarsi dei loop di import

import os, logging


# Creazione directory in cui salvare log (se non già presente)
os.makedirs("../assets", exist_ok=True)

# Istanziazione il logger
logger = logging.getLogger("llm4soc-VMS")
logger.setLevel(logging.INFO)

# Formatter comune
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")

# Handler su file
file_handler = logging.FileHandler("../assets/server.log")
file_handler.setFormatter(formatter)

# Handler su stdout
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

# Aggiunta handler (se non già presenti)
if not logger.hasHandlers():
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)