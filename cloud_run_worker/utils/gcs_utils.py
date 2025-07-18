import os, io, json, asyncio, posixpath
import pandas as pd

from utils.resource_manager import resource_manager as res


# F01 - Costruzione path remoto (usato in VMS per i file 'result', 'metrics' e 'metadata')
def get_blob_path(folder: str, dataset_filename: str, suffix: str, file_format: str) -> str:
    dataset_name = os.path.splitext(dataset_filename)[0]    # Es: "AAA.json" -> "AAA" oppure "AAA" -> "AAA"
    return posixpath.join(folder, f"{dataset_name}_{suffix}.{file_format}")


# F02 - Caricamento del solo chunk d'interesse dal dataset su GCS (previene memory leaks in RAM)
def load_batch(path: str, start_row: int, end_row: int, chunksize: int) -> pd.DataFrame:
    stream = io.BytesIO(res.bucket.blob(path).download_as_bytes())

    batch_data = []
    current_index = 0

    for chunk in pd.read_json(stream, lines=True, chunksize=chunksize):
        chunk_len = len(chunk)
        chunk_start = current_index
        chunk_end = current_index + chunk_len

        if chunk_end <= start_row:
            current_index = chunk_end
            continue
        elif chunk_start >= end_row:
            break

        start_in_chunk = max(0, start_row - chunk_start)
        end_in_chunk = min(chunk_len, end_row - chunk_start)
        batch_data.append(chunk.iloc[start_in_chunk:end_in_chunk])

        current_index = chunk_end

    if not batch_data:
        return pd.DataFrame()

    return pd.concat(batch_data, ignore_index=True)
    # Nota:
    # Questa funzione estrae la sezione di dataset desiderata, senza caricare l'intero dataset in memoria RAM
    # Vecchio codice in 'app,py':
    #   data = res.bucket.blob(dataset_path).download_as_text()
    #   df = pd.read_json(io.StringIO(data), lines=True)
    #   batch_df = df.iloc[start_row:end_row]


# F03 - Upload asincrono di lista di oggetti JSON su GCS
async def upload_as_jsonl(path: str, data: list[dict]):
    await asyncio.to_thread(
        lambda: res.bucket.blob(path).upload_from_string(
            "\n".join(json.dumps(obj) for obj in data),
            content_type="application/json"
        )
    )
    # Nota:
    # Questa funzione asincrona consente di non dover aspettare il termine dell'operazione di upload dati in caso venga ricevuta
    # una seconda richiesta di upload. In questo modo, le operazioni partono in parallelo invece che attendere la fine
    # di quella già in esecuzione.
