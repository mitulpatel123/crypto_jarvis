import json
import httpx
import logging

# Setup
SECRETS_FILE = "gemini_secrets.json"
MODEL = "gemini-flash-latest" 
URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL}:generateContent"
PROXY = "http://iproyal_user:iproyal_pass@geo.iproyal.com:12323" # Will use rotation in real app, here checking keys

def test_keys():
    with open(SECRETS_FILE) as f:
        data = json.load(f)
        keys = data['api_keys']

    print(f"--- Testing {len(keys)} Keys against {MODEL} ---")
    
    # Load Real Proxies
    proxies_list = []
    with open("../iproyal-proxies.txt", "r") as f:
        for line in f:
            if line.strip():
                parts = line.strip().split(':')
                if len(parts) == 4:
                    proxies_list.append(f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}")
    
    for i, item in enumerate(keys):
        key = item['key']
        proxy = proxies_list[i % len(proxies_list)] # Rotate proxy per key test
        
        try:
            with httpx.Client(proxy=proxy, timeout=10.0) as client:
                response = client.post(
                    f"{URL}?key={key}",
                    json={"contents": [{"parts": [{"text": "ping"}]}]},
                    headers={"Content-Type": "application/json"}
                )
                
                status = response.status_code
                if status == 200:
                    print(f"Key {i+1} (...{key[-4:]}): ✅ OK")
                else:
                    print(f"Key {i+1} (...{key[-4:]}): ❌ Failed ({status}) - {response.text[:100]}...")
        except Exception as e:
            print(f"Key {i+1} (...{key[-4:]}): ⚠️ Error ({e})")

if __name__ == "__main__":
    test_keys()
