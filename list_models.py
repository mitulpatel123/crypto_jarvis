import requests
import json
import httpx

API_KEY = "AIzaSyDvUPlUZ9HgAn3AgJAomzm7E9oI5AfMryw" # One of the user's keys
URL = f"https://generativelanguage.googleapis.com/v1beta/models?key={API_KEY}"

# Use Proxy to be safe (matching client behavior)
PROXY = "http://iproyal_user:iproyal_pass@geo.iproyal.com:12323" # Placeholder, will rely on environment or direct if safe.
# Actually, for ListModels, let's try direct first.

try:
    response = requests.get(URL)
    data = response.json()
    print("Available Models:")
    for m in data.get('models', []):
        print(f"- {m['name']}")
        if 'gemini' in m['name']:
             print(f"  Supported: {m.get('supportedGenerationMethods')}")
except Exception as e:
    print(f"Error: {e}")
