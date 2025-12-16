import asyncio
import logging
import json
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import config
import gemini_client
import openrouter_client
from paper_exchange import PaperExchange
import re

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [HeptaCouncil] %(message)s',
    handlers=[
        logging.FileHandler(f"{config.LOG_DIR}/council_ai.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("HeptaCouncil")

# --- Agent Personas ---
AGENTS = {
    "SENTINEL": {
        "role": "Real-time Order Flow Monitor/Scalper",
        "description": "You watch CVD (Cumulative Volume Delta) and OrderBook Imbalance. Identifies absorption and aggression.",
        "focus": ["market_features", "orderflow"],
        "client": "openrouter"
    },
    "QUANT": {
        "role": "Technical Analyst/Swing",
        "description": "Calculates probabilities based on RSI, MACD, Bollinger Bands, and historical volatility.",
        "focus": ["market_features", "momentum"],
        "client": "openrouter"
    },
    "MACRO": {
        "role": "Global Sentiment & News Analyst",
        "description": "Analyzes correlation between heavy weights (BTC/ETH) and external factors (Legacy markets, News).",
        "focus": ["news_sentiment", "correlation"],
        "client": "openrouter"
    },
    "AUDITOR": {
        "role": "Risk Manager & Critic",
        "description": "You HATE losing money. You veto trades if leverage > 50x or Stop Loss is undefined. You force the 'Journals'.",
        "focus": ["risk_limits", "portfolio_state"],
        "client": "openrouter"
    },
    "COMMANDER": {
        "role": "Decision Maker / General",
        "description": "Synthesizes input from Sentinel, Quant, Macro, and Auditor. OUTPUTS the final Trade Signal (Buy/Sell/Hold).",
        "focus": ["synthesis"],
        "client": "openrouter"
    }
}

class TheCouncil:
    def __init__(self):
        self.engine = create_engine(config.DB_URI)
        self.agents = {}
        for name, profile in AGENTS.items():
            if profile['client'] == 'openrouter':
                self.agents[name] = openrouter_client.get_agent(name)
            else:
                self.agents[name] = gemini_client.get_agent(name)
        
        # Phase 3: Paper Exchange Integration
        self.paper_exchange = PaperExchange()
        logger.info("The Council Assembled (Hybrid: Gemini + OpenRouter). Paper Trading Active.")

    def fetch_market_context(self, symbol="btcusdt"):
        """Fetches the latest features (RSI, CVD, etc) for context."""
        try:
            # Try both lower and upper case to specific database quirks
            query = f"""
                SELECT time, feature_group, feature_data 
                FROM market_features 
                WHERE symbol = '{symbol.lower()}' OR symbol = '{symbol.upper()}'
                ORDER BY time DESC LIMIT 20
            """
            df = pd.read_sql(query, self.engine)
            if df.empty:
                logger.warning(f"Feature Context Empty for {symbol}")
                return "No Market Data Available."
                
            # Convert to a summarized string
            context_lines = []
            for _, row in df.iterrows():
                # feature_data is JSONB, simplify it
                context_lines.append(f"[{row['feature_group'].upper()}] {row['time']}: {json.dumps(row['feature_data'])}")
            
            return "\n".join(context_lines[:10]) # Pass last 10 snapshots
        except Exception as e:
             logger.error(f"DB Error: {e}")
             return "Data Unavailable"

    async def gather_intelligence(self, symbol):
        """Asks Sentinel, Quant, and Macro for their analysis in parallel."""
        context = self.fetch_market_context(symbol)
        tasks = []
        
        # 1. Sentinel (Scalper/Orderflow)
        t1 = self.run_agent("SENTINEL", 
            f"Analyze this Order Flow data for {symbol}:\n{context}\nAre buyers or sellers aggressive? Is there absorption?",
            "Report distinct aggression levels."
        )
        # 2. Quant (Technical)
        t2 = self.run_agent("QUANT",
            f"Analyze this Technical Momentum for {symbol}:\n{context}\nWhat is the RSI/MACD setup? Probable direction?",
            "Provide technical probability."
        )
        # 3. Macro (For now, assumes BTC correlation)
        t3 = self.run_agent("MACRO",
            f"Analyze {symbol} in context of broader trend. If BTC is chopping, what should we do?\n{context}",
            "Assess systemic risk."
        )
        
        # Run parallel
        results = await asyncio.gather(t1, t2, t3)
        return {
            "SENTINEL": results[0],
            "QUANT": results[1],
            "MACRO": results[2],
            "context_dump": context
        }

    async def run_agent(self, agent_name, context, goal):
        """Wraps the sync gemini call in async executor"""
        system_prompt = f"You are {agent_name} ({AGENTS[agent_name]['role']}). {AGENTS[agent_name]['description']}"
        loop = asyncio.get_event_loop()
        try:
            # Run blocking HTTP call in thread
            response = await loop.run_in_executor(None, self.agents[agent_name].query, system_prompt, f"CONTEXT: {context}\nGOAL: {goal}")
            if not response:
                return "No Response (API Error)"
            return response
        except Exception as e:
            return f"Agent Error: {e}"

    def parse_and_execute(self, symbol, decision_text, current_price):
        """Parses Commander's output and executes paper trade."""
        try:
            action = "HOLD"
            leverage = 1.0
            
            # Simple Regex Parsing
            if "ACTION: BUY" in decision_text.upper():
                action = "BUY"
            elif "ACTION: SELL" in decision_text.upper():
                action = "SELL"
            
            # Extract Leverage
            lev_match = re.search(r'LEVERAGE:\s*(\d+)x?', decision_text, re.IGNORECASE)
            if lev_match:
                leverage = float(lev_match.group(1))

            if action != "HOLD":
                # Execute Trade ($1000 size for demo per trade)
                result = self.paper_exchange.execute_trade(symbol, action, current_price, 1000.0, leverage)
                logger.info(f"PAPER TRADE EXECUTION: {result}")
                return result
            
            return "HOLDING"
            
        except Exception as e:
            logger.error(f"Execution Error: {e}")
            return f"Error: {e}"

    async def execute_war_room(self, symbol="BTCUSDT"):
        logger.info(f"--- WAR ROOM SESSION STARTED FOR {symbol} ---")
        
        # Phase 1: Intelligence Gathering
        reports = await self.gather_intelligence(symbol)
        
        # Get approx current price from context (hacky but works for now)
        current_price = 90000.0
        try:
             # Try to find last price in context dump
             match = re.search(r'"close":\s*([\d\.]+)', reports['context_dump'])
             if match: current_price = float(match.group(1))
        except: pass

        # Phase 2: Audit (Risk Check)
        auditor_input = f"""
        Review these Agent Reports:
        [SENTINEL]: {reports['SENTINEL']}
        [QUANT]: {reports['QUANT']}
        [MACRO]: {reports['MACRO']}
        
        Current Portfolio Balance: ${self.paper_exchange.get_balance()}
        
        Are there contradictions? Is the risk too high for a trade?
        """
        audit_report = await self.run_agent("AUDITOR", auditor_input, "Veto unsafe conditions. Summarize risk.")
        
        # Phase 3: Commander Decision (WITH REASONING)
        commander_input = f"""
        Final Strategy Required.
        
        [INTELLIGENCE REPORT]
        Sentinel: {reports['SENTINEL']}
        Quant: {reports['QUANT']}
        Macro: {reports['MACRO']}
        
        [RISK AUDIT]
        {audit_report}
        
        TASK:
        1. First, THINK STEP-BY-STEP (Chain of Thought). Weigh the Bullish vs Bearish evidence.
        2. Second, decide the ACTION (BUY / SELL / HOLD).
        3. Third, determine leverage based on confidence (1x - 50x).
        
        OUTPUT FORMAT:
        [REASONING]
        - Point 1
        - Point 2
        ...
        
        [DECISION]
        ACTION: ...
        LEVERAGE: ...
        """
        decision = await self.run_agent("COMMANDER", commander_input, "Issue Final Order.")
        
        logger.info(f"--- COMMANDER DECISION ---\n{decision}\n---------------------------")
        
        # Execute Paper Trade
        self.parse_and_execute(symbol, decision, current_price)
        
        return decision

    async def start_service(self):
        """Runs the War Room Loop continuously."""
        logger.info("Council Service Started. Running every 60 seconds.")
        while True:
            try:
                await self.execute_war_room("btcusdt") # Primary Asset
                # Future: Loop through self.assets
            except Exception as e:
                logger.error(f"War Room Cycle Failed: {e}")
            
            await asyncio.sleep(60)

if __name__ == "__main__":
    council = TheCouncil()
    asyncio.run(council.start_service())
