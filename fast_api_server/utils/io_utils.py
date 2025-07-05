# I/O Utils: modulo per la lettura e scrittura di file locali (VM) e remoti (GCS) 

import os, json

from fastapi import HTTPException
from utils.resource_manager import resource_manager as res


# F01 - Lettura file JSON locale
def read_json(path: str) -> list | dict:    # se nel file c'è un solo oggetto, sarà restituito un 'dict', altrimenti una 'list[dict]'
    if not os.path.exists(path):
        msg = f"[io|F01]\t\t-> File '{path}' not found"
        res.logger.error(msg)
        raise FileNotFoundError(msg)
    
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except Exception as e:
        msg = f"[io|F01]\t\t-> Failed to read JSON in '{path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)
    
    res.logger.info(f"[io|F01]\t\t-> File read from '{path}'")
    return data

# F02 - Scrittura file JSON in locale
def write_json(data: list | dict, path: str):
    try:
        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        res.logger.info(f"[io|F02]\t\t-> File JSON written to '{path}'")
    except Exception as e:
        msg = f"[io|F02]\t\t-> Failed to write JSON to '{path}' ({type(e).__name__}): {str(e)}"
        res.logger.error(msg)
        raise HTTPException(status_code=500, detail=msg)