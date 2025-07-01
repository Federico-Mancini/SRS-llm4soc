#NB: non usare la classe ResourceManager, o potrebbero verificarsi dei loop di import

import logging


logger = logging.getLogger("llm4soc-CRR")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)

if not logger.hasHandlers():
    logger.addHandler(handler)
