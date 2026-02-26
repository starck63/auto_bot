import time, json, os
from dataclasses import dataclass
from datetime import datetime, timezone

import yaml
import ccxt
import pandas as pd

# =========================
# Utilities
# =========================

def load_json(path, default):
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

# =========================
# Indicators
# =========================

def atr(df, period):
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

# =========================
# Strategy
# =========================

@dataclass
class Signal:
    side: str
    structure_sl: float


def detect_reversal_sweep(df, lb):

    if len(df) < lb + 2:
        return None

    i = len(df) - 2
    bar = df.iloc[i]

    recent_low = df["low"].iloc[i-lb:i].min()
    recent_high = df["high"].iloc[i-lb:i].max()

    body = abs(bar["close"] - bar["open"])
    lower_wick = min(bar["open"], bar["close"]) - bar["low"]
    upper_wick = bar["high"] - max(bar["open"], bar["close"])

    # ===== LONG REVERSAL =====
    long_reversal = (
        bar["low"] <= recent_low * 1.001 and
        lower_wick > body * 0.8
    )

    # ===== LONG CONTINUATION =====
    long_continuation = (
        bar["high"] >= recent_high and
        bar["close"] > bar["open"] and
        body > (df["high"] - df["low"]).iloc[i] * 0.4
    )

    # ===== SHORT REVERSAL =====
    short_reversal = (
        bar["high"] >= recent_high * 0.999 and
        upper_wick > body * 0.8
    )

    # ===== SHORT CONTINUATION =====
    short_continuation = (
        bar["low"] <= recent_low and
        bar["close"] < bar["open"] and
        body > (df["high"] - df["low"]).iloc[i] * 0.4
    )

    if long_reversal or long_continuation:
        return Signal("long", bar["low"])

    if short_reversal or short_continuation:
        return Signal("short", bar["high"])

    print("RECENT LOW:", recent_low)
    print("RECENT HIGH:", recent_high)
    print("BAR LOW:", bar["low"])
    print("BAR HIGH:", bar["high"])

    print("BODY:", body)
    print("RANGE:", (bar["high"] - bar["low"]))
    print("BODY > 40% ?", body > (bar["high"] - bar["low"]) * 0.4)
    print("CLOSE < OPEN ?", bar["close"] < bar["open"])

    return None
def position_size(balance, risk_frac, entry, sl):
    risk_usdt = balance * risk_frac
    loss_per_unit = abs(entry - sl)
    if loss_per_unit <= 0:
        return 0
    return risk_usdt / loss_per_unit

# =========================
# OKX Bot
# =========================

class OKXBot:

    def __init__(self, cfg):
        self.cfg = cfg

        self.ex = ccxt.okx({
            "apiKey": os.getenv("OKX_API_KEY"),
            "secret": os.getenv("OKX_SECRET"),
            "password": os.getenv("OKX_PASSWORD"),
            "enableRateLimit": True,
        })

        self.ex.options["defaultType"] = "swap"
        
        if not os.getenv("OKX_API_KEY"):
            raise Exception("API KEY NOT LOADED")        

        self.symbol = cfg["symbol"]
        self.ex.load_markets()

        self.ex.set_leverage(
            cfg["leverage"],
            self.symbol,
            {"mgnMode": cfg["margin_mode"]}
        )

        self.state_path = "./state.json"

        self.state = load_json(self.state_path, {
            "in_position": False,
            "side": None,
            "entry": None,
            "sl": None,
            "tp": None,
            "risk": None,
            "contracts": None,
            "loss_streak": 0,
            "start_equity": None,
            "date": None,
            "be_moved": False
        })

    def fetch_df(self):
        ohlcv = self.ex.fetch_ohlcv(self.symbol, "5m", limit=200)
        return pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","vol"])

    def fetch_balance(self):
        bal = self.ex.fetch_balance()
        return float(bal["USDT"]["free"])

    def check_new_day(self, equity):
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.state["date"] != today:
            self.state["date"] = today
            self.state["start_equity"] = equity
            self.state["loss_streak"] = 0
            save_json(self.state_path, self.state)

    # =========================
    # Main Engine
    # =========================

    def run_once(self):

        df = self.fetch_df()
        equity = self.fetch_balance()
        self.check_new_day(equity)

        if self.state["start_equity"] is None:
            self.state["start_equity"] = equity

                # ===== Risk control =====

        risk_cfg = self.cfg.get("risk", {})
        daily_limit = risk_cfg.get("daily_loss_limit", 0.03)
        max_losses = risk_cfg.get("max_consecutive_losses", 3)

        drawdown = (equity - self.state["start_equity"]) / self.state["start_equity"]

        if drawdown <= -daily_limit:
            print("DAILY LOSS LIMIT HIT")
            return

        if self.state["loss_streak"] >= max_losses:
            print("LOSS STREAK LIMIT HIT")
            return

        df["ema"] = df["close"].ewm(span=34, adjust=False).mean()
        price = df["close"].iloc[-1]
        ema = df["ema"].iloc[-1]

        
        # =========================
        # POSITION MANAGEMENT
        # =========================

        if self.state["in_position"]:
            return

            side = self.state["side"]
            entry = self.state["entry"]
            sl = self.state["sl"]
            tp = self.state["tp"]
            contracts = self.state["contracts"]
            risk = self.state["risk"]

            # ---- BE ----
            if not self.state["be_moved"]:
                if (side=="long" and price >= entry + risk) or \
                   (side=="short" and price <= entry - risk):

                    self.state["sl"] = entry
                    self.state["be_moved"] = True
                    save_json(self.state_path, self.state)
                    print("BE MOVED")
                    return

            # ---- TRAIL ----
            if self.state["be_moved"]:
                if (side=="long" and price >= entry + risk*1.5) or \
                   (side=="short" and price <= entry - risk*1.5):

                    a = atr(df, 14).iloc[-1]

                    if side=="long":
                        new_sl = price - a*1.5
                        if new_sl > self.state["sl"]:
                            self.state["sl"] = new_sl
                    else:
                        new_sl = price + a*1.5
                        if new_sl < self.state["sl"]:
                            self.state["sl"] = new_sl

            # ---- SL ----
            if (side=="long" and price <= self.state["sl"]) or \
               (side=="short" and price >= self.state["sl"]):

                print("STOP LOSS")
                self.ex.create_order(self.symbol,"market",
                                     "sell" if side=="long" else "buy",
                                     contracts)
                self.state["loss_streak"] += 1
                self.state["in_position"] = False
                save_json(self.state_path, self.state)
                return

            # ---- TP ----
            if (side=="long" and price >= tp) or \
               (side=="short" and price <= tp):

                print("TAKE PROFIT")
                self.ex.create_order(self.symbol,"market",
                                     "sell" if side=="long" else "buy",
                                     contracts)
                self.state["loss_streak"] = 0
                self.state["in_position"] = False
                save_json(self.state_path, self.state)
                return

            return
        sig = detect_reversal_sweep(df, self.cfg["swing_lookback"])
        print("LAST BAR:", df.iloc[-3][["open","high","low","close"]])
        print("SIG:", sig)
        
        # =========================
        # ENTRY
        # =========================
        sig = detect_reversal_sweep(df, self.cfg["swing_lookback"])
        if not sig:
            return

        # 반드시 먼저 정의
        sl = float(sig.structure_sl)
        risk = abs(price - sl)
        tp = price + risk*self.cfg["rr"] if sig.side=="long" else price - risk*self.cfg["rr"]

        print("ENTRY:", price)
        print("SL:", sl)
        print("RISK:", risk)

        # ===== 고정 계약 =====
        market = self.ex.market(self.symbol)
        contract_size = market["contractSize"]

        contracts = 0.01

        notional = contracts * contract_size * price
        required_margin = notional / self.cfg["leverage"]

        if required_margin > equity * 0.9:
            print("MARGIN TOO LARGE")
            return
  
        params = {"tdMode": self.cfg["margin_mode"]}

        side_order = "buy" if sig.side=="long" else "sell"

        order = self.ex.create_order(
            self.symbol,
            "market",
            side_order,
            contracts,
            None,
            params
        )

        fill = float(order.get("average") or price)

        self.state.update({
            "in_position": True,
            "side": sig.side,
            "entry": fill,
            "sl": sl,
            "tp": tp,
            "risk": abs(fill - sl),
            "contracts": contracts,
            "be_moved": False
        })

        save_json(self.state_path, self.state)
        print("ENTRY EXECUTED")

        if sig.side == "long" and sl >= price:
            print("INVALID LONG SL")
            return

        if sig.side == "short" and sl <= price:
            print("INVALID SHORT SL")
            return

# =========================
# MAIN
# =========================

def main():
    with open("config.yaml","r") as f:
        cfg = yaml.safe_load(f)

    bot = OKXBot(cfg)

    print("BOT STARTED")
   
    while True:
        try:
            bot.run_once()
        except Exception as e:
            print("ERROR:", e)
        time.sleep(20)

if __name__ == "__main__":
    main()


 





