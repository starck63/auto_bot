//@version=6
indicator("Swing Pullback Entry Clean", overlay=true)

// 기본
emaLen = 50
lookback = 10
atrLen = 14

ema = ta.ema(close, emaLen)
atr = ta.atr(atrLen)

// 추세
trendUp   = close > ema
trendDown = close < ema

// 스윙
swingHigh = ta.highest(high, lookback)
swingLow  = ta.lowest(low, lookback)

// 되돌림 영역
nearHigh = high >= swingHigh[1] * 0.998
nearLow  = low  <= swingLow[1]  * 1.002

// 반전
bearReversal = close < open and (open - close) > atr * 0.4
bullReversal = close > open and (close - open) > atr * 0.4

// 고점 실패
failedHigh = high > high[1] and close < high[1]

// 구조 약화
structureBreak = low < ta.lowest(low[1], 3)

// 하락 추세 되돌림 숏
pullbackShort = trendDown and failedHigh and bearReversal

// 상승 추세 고점 전환 숏
majorShort = trendUp and nearHigh and structureBreak and bearReversal

// 최종 숏
enterShort = majorShort or pullbackShort

// 롱
enterLong = trendDown and nearLow and bullReversal

// 한 번만 표시
shortSignal = enterShort and not enterShort[1]
longSignal  = enterLong  and not enterLong[1]

// 표시
plotshape(shortSignal, style=shape.triangledown, location=location.abovebar, color=color.red, size=size.small)
plotshape(longSignal,  style=shape.triangleup,   location=location.belowbar, color=color.lime, size=size.small)
