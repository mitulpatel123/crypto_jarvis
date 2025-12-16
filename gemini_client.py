import json
import random
import logging
import httpx
from itertools import cycle
import config

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [GeminiCouncil] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/gemini_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("GeminiCouncil")

class KeyRotator:
    def __init__(self):
        self.keys = self._load_keys()
        self.proxies = self._load_proxies()
        
        # Rotators
        self.key_cycle = cycle(self.keys)
        self.proxy_cycle = cycle(self.proxies)
        
        logger.info(f"Loaded {len(self.keys)} Gemini Keys and {len(self.proxies)} Proxies.")

    def _load_keys(self):
        try:
            with open("gemini_secrets.json", "r") as f:
                data = json.load(f)
                keys = data.get("api_keys", [])
                
                # Prioritize Paid Keys
                paid = [k['key'] for k in keys if k.get('type') == 'paid']
                free = [k['key'] for k in keys if k.get('type') == 'free']
                
                # Strategy: Use Paid keys first, then fallback to Free
                final_list = paid + free
                return final_list
        except Exception as e:
            logger.error(f"Failed to load keys: {e}")
            return []

    def _load_proxies(self):
        # Expected format: ip:port:user:pass
        proxies = []
        try:
            with open("../iproyal-proxies.txt", "r") as f:
                for line in f:
                    if line.strip():
                        parts = line.strip().split(':')
                        if len(parts) == 4:
                            # Format for httpx: http://user:pass@ip:port
                            proxies.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")
        except Exception as e:
            logger.error(f"Failed to load proxies: {e}")
        return proxies

    def get_session(self):
        """Returns a (key, proxy_url) tuple for the next request"""
        if not self.keys: raise ValueError("No API Keys available")
        if not self.proxies: raise ValueError("No Proxies available")
        
        return next(self.key_cycle), next(self.proxy_cycle)

class GeminiAgent:
    def __init__(self, name, rotator, model="gemini-flash-latest"): # Alias for 1.5 Flash
        self.name = name
        self.rotator = rotator
        self.model = model
        self.base_url = "https://generativelanguage.googleapis.com/v1beta/models"

    def query(self, system_prompt, user_prompt, retries=5):
        for attempt in range(retries):
            key, proxy = self.rotator.get_session()
            
            # Combine System + User prompt for Gemini (it supports system_instruction but simple concatenation works robustly)
            full_prompt = f"SYSTEM: {system_prompt}\nUSER: {user_prompt}"
            
            payload = {
                "contents": [{
                    "parts": [{"text": full_prompt}]
                }],
                "generationConfig": {
                    "temperature": 0.5,
                    "maxOutputTokens": 2048
                }
            }
            
            url = f"{self.base_url}/{self.model}:generateContent?key={key}"
            
            try:
                # Use HTTPX with Proxy
                # proxy is a single string like http://user:pass@ip:port
                with httpx.Client(proxy=proxy, timeout=30.0) as client:
                    masked_key = f"...{key[-4:]}"
                    proxy_ip = proxy.split('@')[-1]
                    logger.info(f"Agent {self.name} (Att {attempt+1}) querying {self.model} via {proxy_ip} Key {masked_key}")
                    
                    response = client.post(url, json=payload, headers={"Content-Type": "application/json"})
                    
                    if response.status_code != 200:
                        logger.warning(f"Gemini API Error {response.status_code}: {response.text}")
                        continue # Retry with next key
                    
                    data = response.json()
                    # Parse Response
                    try:
                        content = data['candidates'][0]['content']['parts'][0]['text']
                        return content
                    except (KeyError, IndexError) as e:
                        logger.error(f"Response Parsing Failed: {e} - Data: {data}")
                        return None
                        
            except Exception as e:
                logger.error(f"Agent {self.name} Failed: {e}")
                continue
                
        return None

# --- Singleton ---
rotator = KeyRotator()

def get_agent(name):
    return GeminiAgent(name, rotator)

if __name__ == "__main__":
    # Test Run
    agent = get_agent("TestGemini")
    print("Testing Gemini Council...")
    res = agent.query("You are a smart AI.", "Say hello and current model version.")
    print("Response:", res)
