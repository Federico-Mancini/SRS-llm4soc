import json, time, asyncio, datetime
import utils.vertexai_utils as vxc
import utils.gcs_utils as gcs

from google.cloud import storage
from utils.cache_utils import alert_hash, load_alert_cache, save_alert_cache

def mytest():
    print("test")