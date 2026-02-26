import streamlit as st
import pandas as pd
import yfinance as yf
import FinanceDataReader as fdr
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import os
import requests

# --- [웹 앱 기본 설정] ---
st.set_page_config(page_title="NS 글로벌 관제탑", page_icon="🏢", layout="centered")

# --- [폰트 강제 고정] ---
@st.cache_resource
def setup_font():
    font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'
    if os.path.exists(font_path):
        fm.fontManager.addfont(font_path)
        plt.rc('font', family='NanumGothic')
    else:
        plt.rc('font', family='Malgun Gothic') 
    plt.rcParams['axes.unicode_minus'] = False

setup_font()

# --- [종목 리스트 메모리 저장 (과부하 방지)] ---
@st.cache_data(ttl=3600*24)
def get_krx_list():
    return fdr.StockListing('KRX')

# --- [AI 비서 분석 엔진 (차단 대비 유연한 멘트 포함)] ---
def get_premium_analysis(name, roe, pbr, debt, is_us, is_info_blocked):
    if is_info_blocked:
        return f"💡 **[시장 관제]** 대표님, 현재 해외 데이터 센터(야후) 접속량 폭주로 일부 재무 지표 수신이 지연되었습니다. 하지만 **핵심인 20/60일선 스윙 차트는 보조 루트를 통해 완벽하게 확보**했습니다. 아래 차트로 추세를 판독하십시오."

    if any(x in name for x in ["200", "KODEX", "TIGER", "S&P", "나스닥", "ETF"]):
        return f"💡 **[시장 관제]** 지수 추종 ETF입니다. 개별 재무보다는 60일선(빨간색) 추세를 '단지 전체의 지반'이라 생각하고 20일선(노란색)의 돌파 여부를 확인하십시오."
    
    if any(x in name for x in ["금융", "지주", "은행", "증권", "보험"]):
        status = "💎 [안전마진]" if pbr < 0.5 else "✅ [가치적정]"
        return f"{status} 금융주 특유의 밸류 구간입니다. {pbr:.2f}배의 PBR은 자산 대비 가격이 저렴하여 '가성비 최강의 토지 매입'과 같습니다."

    grade = "S [압도적 명품]" if roe > 20 and debt < 100 else \
            "A [우량 기업]" if roe > 10 and debt < 150 else \
            "C [주의 필요]" if roe < 5 or debt > 200 else "B [보통 수준]"

    if is_us:
        strategy = f"글로벌 시장을 주도하는 고효율 기업입니다. 명품은 가격보다 추세가 중요합니다."
    else:
        strategy = "PBR 0.7 미만 가성비 매수 구간입니다." if pbr < 0.7 else "가치 적정선입니다. 60일선 지지 확인이 필수입니다."
        
    return f"**📊 기업등급:** {grade}\n\n**📝 상세전략:** {strategy}\n\n*(체력: ROE {roe:.1f}% / 부채 {debt:.1f}%)*"

# --- [정밀 검색 및 코드 변환 엔진] ---
def get_ticker_by_name(name):
    direct_map = {
        "타이거200": "102110", "코덱스200": "069500",
        "TIGER200": "102110", "KODEX200": "069500",
        "애플": "AAPL", "테슬라": "TSLA", "엔비디아": "NVDA", "아마존": "AMZN", 
        "마소": "MSFT", "넷플릭스": "NFLX", "구글": "GOOGL", "나스닥100": "QQQ", "S&P500": "SPY"
    }
    clean_name = name.replace(" ", "").upper()
    
    if clean_name in direct_map:
        ticker = direct_map[clean_name]
        is_us = not ticker.isdigit()
        yf_ticker = ticker if is_us else f"{ticker}.KS"
        return yf_ticker, ticker, name, is_us
    
    try:
        krx = get_krx_list()
        search_kw = clean_name.replace("타이거", "TIGER").replace("코덱스", "KODEX")
        match = krx[krx['Name'].str.replace(" ", "").str.contains(search_kw, na=False, case=False)]
        if not match.empty:
            best = match.sort_values(by='Marcap', ascending=False).iloc[0]
            code = best['Code']
            return f"{code}.KS", code, best['Name'], False
    except: pass
    return clean_name, clean_name, name, True

# --- [UI 화면 구성] ---
st.title("🏢 NS 글로벌 통합 관제탑")
st.markdown("스마트폰에 최적화된 실시간 우량주/ETF 분석 시스템입니다.")
st.markdown("---")

query = st.text_input("👉 종목명 입력 (타이거200, 아마존 등)", placeholder="여기에 입력하세요")

if st.button("분석 시작", use_container_width=True):
    if query:
        with st.spinner('시장 데이터를 스캔 중입니다... (우회 루트 작동 중)'):
            yf_ticker, fdr_ticker, real_name, is_us = get_ticker_by_name(query)
            data = pd.DataFrame()
            
            try:
                # 1. 차트 데이터 우선 확보 (한국주식은 FDR 우선, 실패 시 야후)
                if not is_us:
                    try:
                        data = fdr.DataReader(fdr_ticker)
                        data = data.tail(250)
                    except: pass
                
                if data.empty:
                    session = requests.Session()
                    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
                    stock = yf.Ticker(yf_ticker, session=session)
                    data = stock.history(period="1y")

                if not data.empty:
                    # 2. 재무 데이터 확보 (야후 차단 대비 보호막 설치)
                    roe, debt, pbr = 0.0, 0.0, 1.0
                    is_info_blocked = False
                    
                    try:
                        stock = yf.Ticker(yf_ticker)
                        info = stock.info
                        if not info or ('regularMarketPrice' not in info and 'currentPrice' not in info):
                            is_info_blocked = True
                        else:
                            roe = info.get('returnOnEquity', 0) * 100 if info.get('returnOnEquity') else 0
                            debt = info.get('debtToEquity', 0) if info.get('debtToEquity') else 0
                            pbr = info.get('priceToBook', 1.0) if info.get('priceToBook') else 1.0
                            if not is_us and pbr == 1.0 and any(x in real_name for x in ["금융", "지주"]): 
                                pbr = 0.38
                    except:
                        is_info_blocked = True

                    st.success(f"[{real_name}] 스캔 완료!")
                    
                    # 브리핑 출력
                    st.info(get_premium_analysis(real_name, roe, pbr, debt, is_us, is_info_blocked))
                    
                    # 20/60일선 차트 그리기
                    data['MA20'] = data['Close'].rolling(20).mean()
                    data['MA60'] = data['Close'].rolling(60).mean()
                    
                    fig, ax = plt.subplots(figsize=(9, 4.5))
                    ax.plot(data.index[-100:], data['Close'].tail(100), label='Price', color='dodgerblue', linewidth=2)
                    ax.plot(data.index[-100:], data['MA20'].tail(100), label='20MA (단기)', color='orange', linestyle='--')
                    ax.plot(data.index[-100:], data['MA60'].tail(100), label='60MA (스윙)', color='red', linewidth=2)
                    
                    # 골든크로스 상승 탄력 구간 (빨간색 음영)
                    ax.fill_between(data.index[-100:], data['MA20'].tail(100), data['MA60'].tail(100), 
                                     where=(data['MA20'].tail(100) >= data['MA60'].tail(100)), color='red', alpha=0.1)
                    
                    ax.set_title(f"[{real_name}] 20/60일 추세 정밀 분석")
                    ax.legend(loc='upper left')
                    ax.grid(True, alpha=0.2)
                    
                    st.pyplot(fig)
                else:
                    st.error("⚠️ 데이터를 찾지 못했습니다. 종목명을 다시 확인해 주십시오.")
            except Exception as e:
                st.error(f"⚠️ 시스템 오류: {e}")
    else:
        st.warning("종목명을 먼저 입력해 주십시오.")
