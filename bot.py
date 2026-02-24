import time, json, math, os
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple, List

import yaml
import ccxt
import pandas as pd

# -----------------------------
# Utilities
# -----------------------------
def now_ms() -> int:
    return int(time.time() * 1000)

def load_json(path: str, default: dict) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def save_json(path: str, obj: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

def append_jsonl(path: str, obj: dict) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

# -----------------------------
# Indicators
# -----------------------------
def ema(series: pd.Series, period: int) -> pd.Series:
    return series.ewm(span=period, adjust=False).mean()

def atr(df: pd.DataFrame, period: int) -> pd.Series:
    # df columns: open, high, low, close
    prev_close = df["close"].shift(1)
    tr = pd.concat([
        (df["high"] - df["low"]),
        (df["high"] - prev_close).abs(),
        (df["low"] - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(period).mean()

def swing_low(df: pd.DataFrame, idx: int, lb: int) -> bool:
    """True if low at idx is lower than lows of lb bars on each side."""
    if idx - lb < 0 or idx + lb >= len(df):
        return False
    center = df["low"].iloc[idx]
    left = df["low"].iloc[idx - lb: idx]
    right = df["low"].iloc[idx + 1: idx + lb + 1]
    return (center < left.min()) and (center < right.min())

def swing_high(df: pd.DataFrame, idx: int, lb: int) -> bool:
    if idx - lb < 0 or idx + lb >= len(df):
        return False
    center = df["high"].iloc[idx]
    left = df["high"].iloc[idx - lb: idx]
    right = df["high"].iloc[idx + 1: idx + lb + 1]
    return (center > left.max()) and (center > right.max())

# -----------------------------
# Strategy Core
# -----------------------------
@dataclass
class Signal:
    side: str               # "long" or "short"
    reason: str
    structure_sl: float     # raw structure SL (before buffer)
    entry_ref: float        # reference entry price (close of trigger bar)

def trend_filter_15m(df15: pd.DataFrame, ema_period: int) -> str:
    """Return 'long', 'short', or 'neutral' based on 15m EMA."""
    if len(df15) < ema_period + 5:
        return "neutral"
    e = ema(df15["close"], ema_period).iloc[-1]
    c = df15["close"].iloc[-1]
    if c > e:
        return "long"
    if c < e:
        return "short"
    return "neutral"

def detect_reversal_sweep_5m(df5: pd.DataFrame, lb: int) -> Optional[Signal]:
    """
    현실형 트리거(대표님 OB/FVG 붙이기 쉬운 형태):
    - (롱) 최근 스윙 로우를 한번 더 갱신(스윕)한 뒤, 현재 봉이 양봉 마감(반전)
    - (숏) 최근 스윙 하이를 갱신한 뒤, 현재 봉이 음봉 마감
    """
    if len(df5) < (lb * 3 + 30):
        return None

    i = len(df5) - 2  # 마지막 봉은 진행 중일 수 있으니 -2를 확정봉으로 사용
    bar = df5.iloc[i]
    prev = df5.iloc[i - 1]

    # 최근 스윙 포인트 탐색(가장 가까운 것)
    last_swing_low_idx = None
    last_swing_high_idx = None
    for k in range(i - 2, lb, -1):
        if last_swing_low_idx is None and swing_low(df5, k, lb):
            last_swing_low_idx = k
        if last_swing_high_idx is None and swing_high(df5, k, lb):
            last_swing_high_idx = k
        if last_swing_low_idx is not None and last_swing_high_idx is not None:
            break

    if last_swing_low_idx is None or last_swing_high_idx is None:
        return None

    last_sw_low = df5["low"].iloc[last_swing_low_idx]
    last_sw_high = df5["high"].iloc[last_swing_high_idx]

    # 롱 스윕+반전: 저점 살짝 깨고(스윕) 양봉 마감
    if bar["low"] < last_sw_low and bar["close"] > bar["open"]:
        structure_sl = bar["low"]  # 구조SL은 '스윕 꼬리 끝' 기준 (후에 버퍼 적용)
        return Signal(
            side="long",
            reason=f"sweep_low_reversal(swing_lb={lb})",
            structure_sl=structure_sl,
            entry_ref=bar["close"]
        )

    # 숏 스윕+반전: 고점 살짝 깨고(스윕) 음봉 마감
    if bar["high"] > last_sw_high and bar["close"] < bar["open"]:
        structure_sl = bar["high"]
        return Signal(
            side="short",
            reason=f"sweep_high_reversal(swing_lb={lb})",
            structure_sl=structure_sl,
            entry_ref=bar["close"]
        )

    return None

def apply_3tick_rule(df5: pd.DataFrame, sig: Signal, tick: float) -> bool:
    """
    3틱룰(확정):
    - 트리거 확정봉 다음 봉(또는 이후)에서 유리방향으로 3틱 이상 진행이 '한 번'이라도 나오면 인정.
    (대표님 대화 흐름: 순간 신호/리페인트 방지 목적)
    """
    i = len(df5) - 2  # 트리거 봉(확정)
    trigger_close = df5["close"].iloc[i]
    next_bar = df5.iloc[i + 1] if i + 1 < len(df5) else None
    if next_bar is None:
        return False

    three = 3.0 * tick
    if sig.side == "long":
        return next_bar["high"] >= (trigger_close + three)
    else:
        return next_bar["low"] <= (trigger_close - three)

def compute_sl_tp(sig: Signal, fill_price: float, df5: pd.DataFrame, cfg: dict, tick: float) -> Tuple[float, float]:
    """
    구조SL + 버퍼( max(2틱, ATR*0.10) ) → 최종 SL
    TP는 체결가 기준 RR=2로 재계산
    """
    atr_period = cfg["stops"]["atr_period"]
    k = cfg["stops"]["buffer_atr_k"]
    min_ticks = cfg["stops"]["buffer_min_ticks"]

    a = atr(df5, atr_period).iloc[-2]  # 확정봉 기준
    buf = max(min_ticks * tick, float(a) * k)

    if sig.side == "long":
        sl = sig.structure_sl - buf
        risk = fill_price - sl
        tp = fill_price + cfg["risk"]["rr"] * risk
    else:
        sl = sig.structure_sl + buf
        risk = sl - fill_price
        tp = fill_price - cfg["risk"]["rr"] * risk

    return sl, tp

def position_size(balance_usdt: float, risk_frac: float, fill: float, sl: float, contract_value: float = 1.0) -> float:
    """
    1% 리스크 고정 수량 계산.
    contract_value는 상품별 계약 단위를 반영해야 함.
    OKX USDT-SWAP의 경우 ccxt market info 기반으로 조정 권장.
    """
    risk_usdt = balance_usdt * risk_frac
    per_unit_loss = abs(fill - sl) * contract_value
    if per_unit_loss <= 0:
        return 0.0
    qty = risk_usdt / per_unit_loss
    return qty

# -----------------------------
# OKX Execution Layer
# -----------------------------
class OKXBot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.ex = ccxt.okx({
            "apiKey": os.environ.get("OKX_API_KEY", ""),
            "secret": os.environ.get("OKX_API_SECRET", ""),
            "password": os.environ.get("OKX_API_PASSPHRASE", ""),
            "enableRateLimit": True,
            "options": {"defaultType": cfg.get("market_type", "swap")}
        })
        self.symbol = cfg["symbol"]
        self.state_path = cfg["logging"]["state_path"]
        self.log_path = cfg["logging"]["trade_log_path"]
        self.state = load_json(self.state_path, default={
            "bot_enabled": True,
            "day_start_equity": None,
            "consecutive_losses": 0,
            "cooldown_until_ms": 0,
            "in_position": False,
            "side": None,
            "entry": None,
            "sl": None,
            "tp": None,
            "pos_size": None,
            "last_trade_ts": None
        })

        if not (self.ex.apiKey and self.ex.secret and self.ex.password):
            raise RuntimeError("OKX API credentials missing. Set OKX_API_KEY / OKX_API_SECRET / OKX_API_PASSPHRASE.")

        self.ex.load_markets()

    def fetch_ohlcv_df(self, timeframe: str, limit: int = 200) -> pd.DataFrame:
        ohlcv = self.ex.fetch_ohlcv(self.symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "vol"])
        df["ts"] = pd.to_datetime(df["ts"], unit="ms")
        return df

    def get_tick_size(self) -> float:
        m = self.ex.market(self.symbol)
        # OKX tick: priceIncrement / precision
        tick = None
        if "precision" in m and m["precision"].get("price") is not None:
            # If precision is decimals, tick can be 10^-decimals
            tick = 10 ** (-m["precision"]["price"])
        # fallback
        if tick is None:
            tick = 0.1
        return float(tick)

    def fetch_balance_usdt(self) -> float:
        bal = self.ex.fetch_balance()
        # OKX: USDT in total/free may exist depending on account type
        total = bal.get("total", {}).get("USDT", None)
        if total is None:
            # fallback: try free
            total = bal.get("free", {}).get("USDT", 0.0)
        return float(total or 0.0)

    def daily_guard(self, equity: float) -> bool:
        # Initialize day_start_equity
        if self.state["day_start_equity"] is None:
            self.state["day_start_equity"] = equity
            return True

        # daily loss check
        dd = (equity - self.state["day_start_equity"]) / self.state["day_start_equity"]
        if dd <= -self.cfg["risk"]["daily_loss_limit"]:
            self.state["bot_enabled"] = False
            append_jsonl(self.log_path, {"ts": now_ms(), "event": "DAILY_LOSS_LIMIT_HIT", "equity": equity, "dd": dd})
            return False
        return True

    def cooldown_guard(self) -> bool:
        return now_ms() >= int(self.state.get("cooldown_until_ms", 0))

    def set_cooldown(self):
        mins = int(self.cfg["risk"]["cooldown_minutes"])
        self.state["cooldown_until_ms"] = now_ms() + mins * 60 * 1000

    def place_market_order(self, side: str, amount: float) -> Dict[str, Any]:
        # side: "buy" or "sell"
        params = {
            "tdMode": self.cfg["margin_mode"], # isolated
            "leverage": self.cfg["leverage"],
        }
        return self.ex.create_order(self.symbol, "market", side, amount, None, params)

    def place_sl_tp(self, side: str, amount: float, sl: float, tp: float):
        """
        OKX는 조건주문/알고주문 형태가 다양합니다.
        계정/마켓 타입에 따라 파라미터가 달라질 수 있어
        여기서는 '구조'만 잡아두고, 실제 OKX algo 주문 파라미터는 운영 환경에 맞게 보정합니다.
        """
        # 최소 안전: 봇 내부에서 모니터링하며 SL/TP 도달 시 시장가 청산
        # (OKX 서버측 SL/TP는 다음 단계에서 OKX algo 주문 파라미터 확정 후 붙이는 게 안전)
        self.state["sl"] = sl
        self.state["tp"] = tp

    def close_position_market(self, side: str, amount: float):
        # If long, close by sell. If short, close by buy.
        close_side = "sell" if side == "long" else "buy"
        params = {"tdMode": self.cfg["margin_mode"]}
        return self.ex.create_order(self.symbol, "market", close_side, amount, None, params)

    def run_once(self):
        # save state frequently
        tick = self.get_tick_size()

        # fetch market data
        df5 = self.fetch_ohlcv_df(self.cfg["timeframes"]["entry"], limit=220)
        df15 = self.fetch_ohlcv_df(self.cfg["timeframes"]["trend"], limit=220)

        equity = self.fetch_balance_usdt()
        if not self.daily_guard(equity):
            save_json(self.state_path, self.state)
            return

        if not self.state.get("bot_enabled", True):
            save_json(self.state_path, self.state)
            return

        if not self.cooldown_guard():
            save_json(self.state_path, self.state)
            return

        # If in position, manage exits (internal monitor)
        if self.state.get("in_position", False):
            last = df5.iloc[-1]
            px = float(last["close"])
            sl = float(self.state["sl"])
            tp = float(self.state["tp"])
            side = self.state["side"]
            amt = float(self.state["pos_size"])

            hit_sl = (px <= sl) if side == "long" else (px >= sl)
            hit_tp = (px >= tp) if side == "long" else (px <= tp)

            if hit_sl or hit_tp:
                self.close_position_market(side, amt)
                result = "TP" if hit_tp else "SL"
                # update streak/cooldown
                if result == "SL":
                    self.state["consecutive_losses"] = int(self.state.get("consecutive_losses", 0)) + 1
                    if self.state["consecutive_losses"] >= self.cfg["risk"]["max_consecutive_losses"]:
                        self.set_cooldown()
                else:
                    self.state["consecutive_losses"] = 0

                append_jsonl(self.log_path, {
                    "ts": now_ms(), "event": "EXIT", "result": result,
                    "side": side, "exit_px": px, "sl": sl, "tp": tp,
                    "equity": equity
                })

                # reset position state
                self.state.update({
                    "in_position": False, "side": None, "entry": None, "sl": None, "tp": None, "pos_size": None,
                    "last_trade_ts": now_ms()
                })

            save_json(self.state_path, self.state)
            return

        # Not in position: generate signal
        trend = trend_filter_15m(df15, self.cfg["filters"]["trend_ema_period"])
        sig = detect_reversal_sweep_5m(df5, self.cfg["stops"]["swing_lookback"])
        if sig is None:
            save_json(self.state_path, self.state)
            return

        # trend filter
        if self.cfg["filters"]["only_trade_with_trend"]:
            if trend != sig.side:
                save_json(self.state_path, self.state)
                return

        # 3-tick confirmation
        if not apply_3tick_rule(df5, sig, tick):
            save_json(self.state_path, self.state)
            return

        # Slippage guard (optional): if last price moved too far vs entry_ref, wait
        last_px = float(df5["close"].iloc[-1])
        guard_bps = float(self.cfg["execution"]["slippage_guard_bps"])
        if guard_bps > 0:
            diff_bps = abs(last_px - sig.entry_ref) / sig.entry_ref * 10000.0
            if diff_bps > guard_bps:
                save_json(self.state_path, self.state)
                return

        # Place market order
        side_order = "buy" if sig.side == "long" else "sell"

        # preliminary: use last price as fill proxy to compute size, then re-calc with actual fill
        # NOTE: for swaps, contract_value should use OKX contract specs. We'll approximate 1.0 here.
        tmp_sl, _ = compute_sl_tp(sig, last_px, df5, self.cfg, tick)
        qty = position_size(equity, self.cfg["risk"]["risk_per_trade"], last_px, tmp_sl, contract_value=1.0)
        # sanity
        if qty <= 0:
            save_json(self.state_path, self.state)
            return

        order = self.place_market_order(side_order, qty)

        # Fetch fill price best effort
        fill = None
        try:
            # ccxt returns average sometimes
            fill = float(order.get("average") or order.get("price") or last_px)
        except Exception:
            fill = last_px

        sl, tp = compute_sl_tp(sig, fill, df5, self.cfg, tick)

        # Register SL/TP (internal monitor here; OKX algo SL/TP can be added later)
        self.place_sl_tp(sig.side, qty, sl, tp)

        # Update state
        self.state.update({
            "in_position": True,
            "side": sig.side,
            "entry": fill,
            "pos_size": qty,
            "last_trade_ts": now_ms()
        })

        append_jsonl(self.log_path, {
            "ts": now_ms(), "event": "ENTRY",
            "side": sig.side, "reason": sig.reason,
            "fill": fill, "qty": qty, "sl": sl, "tp": tp,
            "trend": trend, "equity": equity
        })

        save_json(self.state_path, self.state)

def main():
    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    bot = OKXBot(cfg)
    poll = int(cfg["execution"]["poll_seconds"])
    print("OKX Bot started. Poll seconds:", poll)

    while True:
        try:
            bot.run_once()
        except ccxt.NetworkError as e:
            append_jsonl(cfg["logging"]["trade_log_path"], {"ts": now_ms(), "event": "NETWORK_ERROR", "msg": str(e)})
        except ccxt.ExchangeError as e:
            append_jsonl(cfg["logging"]["trade_log_path"], {"ts": now_ms(), "event": "EXCHANGE_ERROR", "msg": str(e)})
        except Exception as e:
            append_jsonl(cfg["logging"]["trade_log_path"], {"ts": now_ms(), "event": "FATAL", "msg": str(e)})
        time.sleep(poll)

if __name__ == "__main__":
    main()
