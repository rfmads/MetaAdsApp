# db/config_store.py
from db.db import query_dict, execute
from logs.logger import logger
import requests
# A local cache for all configuration keys
_CONFIG_CACHE = {}

def get_config(key: str, force_refresh: bool = False) -> str:
    """
    Get any config value (META_USER_TOKEN, META_GRAPH_VERSION, etc.) 
    dynamically from the DB or cache.
    """
    global _CONFIG_CACHE
    
    if key in _CONFIG_CACHE and not force_refresh:
        return _CONFIG_CACHE[key]

    sql = "SELECT config_value FROM sys_config WHERE config_key = %s"
    res = query_dict(sql, (key,))
    
    if res:
        val = res[0]['config_value']
        _CONFIG_CACHE[key] = val
        return val
    
    logger.error(f"❌ Configuration key '{key}' not found in sys_config table!")
    return None

def set_config(key: str, value: str):
    """
    Updates or inserts a config key in the database and updates the cache.
    """
    if not key or value is None:
        raise ValueError("Key and Value must be provided")
    
    global _CONFIG_CACHE
    
    sql = """
    INSERT INTO sys_config (config_key, config_value) 
    VALUES (%s, %s) 
    ON DUPLICATE KEY UPDATE config_value = VALUES(config_value)
    """
    execute(sql, (key, value))
    
    _CONFIG_CACHE[key] = value
    logger.info(f"✅ Config '{key}' updated successfully.")

def get_meta_token():
    """Helper for the most common use case"""
    return get_config("META_USER_TOKEN")

def get_graph_version():
    """Helper for the graph version"""
    return get_config("META_GRAPH_VERSION") or "v24.0"

def get_valid_meta_token():
    token = get_meta_token() # Your existing DB fetch
    
    if not is_token_valid(token):
        # Trigger an alert here! (Slack, Email, or SMS)
        logger.critical("🚨 THE SYSTEM TOKEN HAS EXPIRED OR IS INVALID!")
        return None
        
    return token

def get_page_id():
    """Dynamically fetch the active Facebook Page ID"""
    return get_config("PAGE_ID")

def is_token_valid(token: str) -> bool:
    """
    Checks if the token is alive by calling the /me endpoint.
    """
    if not token:
        return False
        
    url = "https://graph.facebook.com/me"
    params = {
        "access_token": token,
        "fields": "id,name"
    }
    
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code == 200:
            return True
        
        # If not 200, log the error details (expired, revoked, etc.)
        error_data = r.json().get("error", {})
        logger.warning(f"⚠️ Token Validation Failed: {error_data.get('message')}")
        return False
    except Exception as e:
        logger.error(f"❌ Connection error during token check: {e}")
        return False
    
    
    