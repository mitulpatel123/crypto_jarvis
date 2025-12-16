import time
import random
import logging
import requests
import warnings
import config
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [NewsFeed] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/news_feed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("NewsFeed")

# --- Configuration ---
PROXIES = {
    "http": "http://127.0.0.1:8080",
    "https": "http://127.0.0.1:8080",
}

# Target URLs
TARGETS_NEWS = [
    "https://cointelegraph.com",
    "https://cryptopanic.com",
    "https://www.coindesk.com",
    "https://decrypt.co",
    "https://beincrypto.com",
    "https://thedefiant.io",
    "https://blockworks.co"
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0"
]

def create_driver():
    options = Options()
    options.add_argument("--headless=new") # Modern headless mode
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    options.add_argument(f"--proxy-server={PROXIES['http']}")
    options.add_argument("--window-size=1920,1080")
    
    # Suppress logs
    logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    # Stealth hacks
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def random_behavior(driver):
    """Simulates vague human behavior to trigger dynamic content loading"""
    try:
        # 1. Random Scroll
        height = driver.execute_script("return document.body.scrollHeight")
        target_scroll = random.randint(300, min(height, 2000))
        driver.execute_script(f"window.scrollTo(0, {target_scroll});")
        time.sleep(random.uniform(0.5, 1.5))
        
        # 2. Mouse Move (ActionChains)
        action = ActionChains(driver)
        action.move_by_offset(random.randint(10, 100), random.randint(10, 100)).perform()
    except Exception as e:
        logger.debug(f"Behavior simulation error: {e}")

def fetch_cycle():
    logger.info("Starting Browser Bot Cycle...")
    
    # Randomize order but cover ALL targets to fix "Stalled" issues
    queue = TARGETS_NEWS[:]
    random.shuffle(queue)
    
    driver = None
    try:
        driver = create_driver()
        
        for target in queue:
            logger.info(f"Visiting {target}...")
            try:
                driver.get(target)
                
                # Wait for initial load
                time.sleep(random.uniform(2, 4))
                
                # Simulate interacting to trigger lazy loading / anti-bot
                random_behavior(driver)
                
                # Allow time for MITM to intercept requests
                time.sleep(random.uniform(3, 5))
                
            except Exception as e:
                logger.error(f"Failed to visit {target}: {e}")
                # Re-create driver if it crashes
                try:
                    driver.quit()
                    driver = create_driver()
                except: pass

    except Exception as e:
        logger.error(f"Critical Driver Error: {e}")
    finally:
        if driver:
            driver.quit()
            
    logger.info("Cycle Complete. Sleeping...")

if __name__ == "__main__":
    # Disable SSL warnings
    requests.packages.urllib3.disable_warnings()
    
    logger.info("Browser Bot Online [Enhanced]")
    while True:
        fetch_cycle()
        # Run every 3-5 minutes
        sleep_time = random.randint(180, 300)
        logger.info(f"Next cycle in {sleep_time} seconds.")
        time.sleep(sleep_time)
