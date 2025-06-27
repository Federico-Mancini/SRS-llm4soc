import json, time, asyncio, datetime
print("IMPORT 1 OK")
import utils.vertexai_utils as vxc
print("IMPORT 2 OK")
import utils.gcs_utils as gcs
print("IMPORT 3 OK")
from google.cloud import storage
print("IMPORT 4 OK")
from utils.cache_utils import alert_hash, load_alert_cache, save_alert_cache
print("IMPORT 5 OK")

def mytest():
    print("test")