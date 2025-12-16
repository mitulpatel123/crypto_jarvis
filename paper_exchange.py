import json
import os
import time
import logging
from datetime import datetime

class PaperExchange:
    def __init__(self, initial_balance=5000.0, portfolio_file="paper_portfolio.json"):
        self.portfolio_file = portfolio_file
        self.initial_balance = initial_balance
        self.logger = logging.getLogger("PaperExchange")
        self.load_portfolio()

    def load_portfolio(self):
        if os.path.exists(self.portfolio_file):
            try:
                with open(self.portfolio_file, 'r') as f:
                    self.portfolio = json.load(f)
            except json.JSONDecodeError:
                self.logger.error("Corrupt portfolio file. Resetting.")
                self.reset_portfolio()
        else:
            self.reset_portfolio()

    def reset_portfolio(self):
        self.portfolio = {
            "balance": self.initial_balance,
            "positions": {}, # symbol: { 'size': 0, 'entry_price': 0, 'side': 'LONG/SHORT' }
            "history": []
        }
        self.save_portfolio()

    def save_portfolio(self):
        with open(self.portfolio_file, 'w') as f:
            json.dump(self.portfolio, f, indent=4)

    def get_balance(self):
        return self.portfolio["balance"]

    def get_position(self, symbol):
        return self.portfolio["positions"].get(symbol, None)

    def execute_trade(self, symbol, action, price, amount_usd, leverage=1.0):
        """
        Executes a paper trade.
        action: 'BUY' (Long) or 'SELL' (Short)
        """
        self.load_portfolio() # Refresh state
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Simple Fee Model (0.05% taker)
        fee = amount_usd * 0.0005 
        cost = amount_usd / leverage
        
        if cost + fee > self.portfolio["balance"]:
            return {"status": "error", "message": "Insufficient Balance"}

        # Calculate Quantity
        quantity = (amount_usd * leverage) / price
        
        # Record Trade
        trade_record = {
            "time": timestamp,
            "symbol": symbol,
            "action": action,
            "price": price,
            "amount_usd": amount_usd,
            "leverage": leverage,
            "fee": fee
        }
        self.portfolio["history"].append(trade_record)
        self.portfolio["balance"] -= fee
        
        # Update Position (Simplified: Netting Mode)
        current_pos = self.portfolio["positions"].get(symbol)
        
        if current_pos is None:
            # New Position
            self.portfolio["positions"][symbol] = {
                "size": quantity if action == 'BUY' else -quantity,
                "entry_price": price,
                "leverage": leverage
            }
        else:
            # Add to existing (or close) - simplified logic for demo
            # A real engine would handle PnL realization here.
            # For now, we'll just track raw size change
            new_size = current_pos["size"] + (quantity if action == 'BUY' else -quantity)
            
            # Weighted Average Entry Price would go here
            self.portfolio["positions"][symbol]["size"] = new_size
            
            # If closed (approx zero)
            if abs(new_size) < 0.00001:
                del self.portfolio["positions"][symbol]

        self.save_portfolio()
        return {"status": "success", "fill_price": price}

    def get_live_pnl(self, current_prices):
        """
        Calculate Unrealized PnL based on current market prices.
        current_prices: dict { 'BTCUSDT': 90000, ... }
        """
        total_pnl = 0
        details = {}
        
        for symbol, pos in self.portfolio["positions"].items():
            if symbol in current_prices:
                current_price = current_prices[symbol]
                entry_price = pos["entry_price"]
                size = pos["size"]
                
                # Long PnL: (Current - Entry) * Size
                # Short PnL: (Entry - Current) * abs(Size)
                if size > 0:
                    pnl = (current_price - entry_price) * size
                else:
                    pnl = (entry_price - current_price) * abs(size)
                    
                total_pnl += pnl
                details[symbol] = pnl
        
        return total_pnl, details
