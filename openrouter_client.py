import json
import random
import logging
import httpx
from itertools import cycle
import config

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [OpenRouterCouncil] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/openrouter_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("OpenRouterCouncil")

class KeyRotator:
    def __init__(self):
        self.keys = self._load_keys()
        self.proxies = self._load_proxies()
        
        # Shuffle for randomness
        random.shuffle(self.keys)
        random.shuffle(self.proxies)

        # Rotators
        self.key_cycle = cycle(self.keys)
        self.proxy_cycle = cycle(self.proxies)
        
        logger.info(f"Loaded {len(self.keys)} OpenRouter Keys and {len(self.proxies)} Proxies.")

    def _load_keys(self):
        try:
            with open("openrouter_secrets.json", "r") as f:
                data = json.load(f)
                keys = data.get("api_keys", [])
                return keys
        except Exception as e:
            logger.error(f"Failed to load keys: {e}")
            return []

    def _load_proxies(self):
        proxies = []
        try:
            with open("../iproyal-proxies.txt", "r") as f:
                for line in f:
                    if line.strip():
                        parts = line.strip().split(':')
                        if len(parts) == 4:
                            proxies.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")
        except Exception as e:
            logger.error(f"Failed to load proxies: {e}")
        return proxies

    def get_session(self):
        if not self.keys: raise ValueError("No API Keys available")
        if not self.proxies: raise ValueError("No Proxies available")
        return next(self.key_cycle), next(self.proxy_cycle)

class OpenRouterAgent:
    def __init__(self, name, rotator, model="google/gemma-3-27b-it:free"):
        self.name = name
        self.rotator = rotator
        self.model = model
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"

    def query(self, system_prompt, user_prompt, retries=5):
        for attempt in range(retries):
            key, proxy = self.rotator.get_session()
            
            headers = {
                "Authorization": f"Bearer {key}",
                "HTTP-Referer": "https://crypto-jarvis.internal", # Required by OpenRouter
                "X-Title": "CryptoJarvis",                        # Required by OpenRouter
                "Content-Type": "application/json"
            }
            
            payload = {
                "model": self.model,
                "messages": [
                    {"role": "user", "content": f"SYSTEM INSTRUCTION: {system_prompt}\n\nUSER QUERY: {user_prompt}"}
                ],
                "temperature": 0.5,
                "max_tokens": 2000
            }
            
            try:
                # Use HTTPX with Proxy
                with httpx.Client(proxy=proxy, timeout=40.0) as client:
                    masked_key = f"...{key[-10:]}"
                    proxy_ip = proxy.split('@')[-1]
                    logger.info(f"Agent {self.name} (Att {attempt+1}) querying {self.model} via {proxy_ip} Key {masked_key}")
                    
                    response = client.post(self.base_url, json=payload, headers=headers)
                    
                    if response.status_code != 200:
                        logger.warning(f"OpenRouter API Error {response.status_code}: {response.text[:200]}")
                        continue 
                    
                    data = response.json()
                    # Parse Response
                    try:
                        content = data['choices'][0]['message']['content']
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
    return OpenRouterAgent(name, rotator)

if __name__ == "__main__":
    # Test Run
    agent = get_agent("TestOpenRouter")
    print("Testing OpenRouter Council...")
    res = agent.query("You are a smart AI.", "Say hello and current model version.")
    print("Response:", res)
