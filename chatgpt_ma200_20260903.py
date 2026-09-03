from __future__ import annotations

import concurrent.futures as futures
import io
import json
import math
import random
import re
import time
import urllib.parse
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

OUT_DIR = Path('ma200_output')
OUT_DIR.mkdir(parents=True, exist_ok=True)

CONSTITUENTS_URL = (
    'https://raw.githubusercontent.com/chinobing/'
    'historical_sp500_constituents/main/sp500_constituents.csv'
)
YAHOO_HOSTS = ['query1.finance.yahoo.com', 'query2.finance.yahoo.com']
USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/152.0.0.0 Safari/537.36'
)
HEADERS = {'User-Agent': USER_AGENT, 'Accept': 'application/json,text/plain,*/*'}

SECTOR_ZH = {
    'Communication Services': '通信服务',
    'Consumer Discretionary': '可选消费',
    'Consumer Staples': '日常消费',
    'Energy': '能源',
    'Financials': '金融',
    'Health Care': '医疗保健',
    'Industrials': '工业',
    'Information Technology': '信息技术',
    'Materials': '原材料',
    'Real Estate': '房地产',
    'Utilities': '公用事业',
}

MANUAL_ZH = {
    'MMM': '3M公司', 'AOS': 'A.O.史密斯', 'ABT': '雅培', 'ABBV': '艾伯维',
    'ACN': '埃森哲', 'ADBE': '奥多比', 'AMD': '超威半导体', 'AES': '爱依斯电力',
    'AFL': '美国人寿保险公司', 'A': '安捷伦科技', 'APD': '空气产品公司',
    'ABNB': '爱彼迎', 'AKAM': '阿卡迈科技', 'ALB': '雅保公司', 'GOOGL': '谷歌A类股',
    'GOOG': '谷歌C类股', 'MO': '奥驰亚集团', 'AMZN': '亚马逊', 'AEP': '美国电力',
    'AXP': '美国运通', 'AIG': '美国国际集团', 'AMT': '美国电塔', 'AMGN': '安进',
    'ADI': '亚德诺半导体', 'AON': '怡安', 'APA': '阿帕奇公司', 'APO': '阿波罗全球管理',
    'AAPL': '苹果公司', 'AMAT': '应用材料', 'APP': '爱普洛文', 'T': '美国电话电报公司',
    'ADSK': '欧特克', 'ADP': '自动数据处理公司', 'AZO': '汽车地带', 'AXON': 'Axon企业',
    'BKR': '贝克休斯', 'BAC': '美国银行', 'BDX': '碧迪医疗', 'BRK.B': '伯克希尔·哈撒韦B类股',
    'BLK': '贝莱德', 'BX': '黑石集团', 'XYZ': 'Block公司', 'BA': '波音', 'BKNG': '缤客控股',
    'BMY': '百时美施贵宝', 'AVGO': '博通', 'C': '花旗集团', 'CAT': '卡特彼勒',
    'CVX': '雪佛龙', 'CMG': '墨式烧烤', 'CI': '信诺集团', 'CSCO': '思科', 'KO': '可口可乐',
    'COIN': 'Coinbase', 'COP': '康菲石油', 'COST': '好市多', 'CRM': '赛富时',
    'CRWD': 'CrowdStrike', 'CVS': '西维斯健康', 'DHR': '丹纳赫', 'DE': '迪尔公司',
    'DELL': '戴尔科技', 'DIS': '华特迪士尼', 'DOW': '陶氏公司', 'DUK': '杜克能源',
    'EBAY': '易贝', 'EA': '艺电', 'EMR': '艾默生电气', 'EOG': 'EOG能源',
    'EQIX': '易昆尼克斯', 'XOM': '埃克森美孚', 'FDX': '联邦快递', 'F': '福特汽车',
    'FCX': '自由港麦克莫兰', 'GD': '通用动力', 'GE': '通用电气', 'GEHC': '通用电气医疗',
    'GEV': '通用电气维诺瓦', 'GM': '通用汽车', 'GILD': '吉利德科学', 'GS': '高盛',
    'HAL': '哈里伯顿', 'HD': '家得宝', 'HON': '霍尼韦尔', 'IBM': '国际商业机器公司',
    'INTC': '英特尔', 'INTU': '财捷', 'ISRG': '直觉外科', 'JNJ': '强生', 'JPM': '摩根大通',
    'KDP': '克里格胡椒博士', 'KHC': '卡夫亨氏', 'KR': '克罗格', 'LRCX': '泛林集团',
    'LLY': '礼来', 'LMT': '洛克希德·马丁', 'LOW': '劳氏', 'LULU': '露露乐蒙',
    'MA': '万事达卡', 'MAR': '万豪国际', 'MCD': '麦当劳', 'MDT': '美敦力',
    'META': 'Meta平台', 'MGM': '美高梅国际酒店集团', 'MCO': '穆迪', 'MS': '摩根士丹利',
    'MSFT': '微软', 'MU': '美光科技', 'NFLX': '奈飞', 'NKE': '耐克', 'NOC': '诺斯罗普·格鲁曼',
    'NOW': 'ServiceNow', 'NVDA': '英伟达', 'ORCL': '甲骨文', 'PANW': '派拓网络',
    'PEP': '百事公司', 'PFE': '辉瑞', 'PM': '菲利普莫里斯国际', 'PG': '宝洁',
    'PLTR': '帕兰蒂尔', 'PYPL': '贝宝', 'QCOM': '高通', 'RTX': 'RTX公司',
    'SBUX': '星巴克', 'SCHW': '嘉信理财', 'SPGI': '标普全球', 'TGT': '塔吉特',
    'TMO': '赛默飞世尔科技', 'TMUS': '美国无线通信', 'TSLA': '特斯拉', 'TXN': '德州仪器',
    'UBER': '优步', 'UNH': '联合健康集团', 'UNP': '联合太平洋', 'UPS': '联合包裹服务',
    'V': '维萨', 'VZ': '威瑞森', 'WMT': '沃尔玛', 'WFC': '富国银行', 'WBD': '华纳兄弟探索',
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def request_json(url: str, params: dict[str, Any] | None = None, tries: int = 5) -> Any:
    last: Exception | None = None
    for attempt in range(tries):
        try:
            r = SESSION.get(url, params=params, timeout=35)
            if r.status_code in (429, 502, 503, 504):
                raise RuntimeError(f'HTTP {r.status_code}')
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            if attempt + 1 < tries:
                time.sleep((1.4 ** attempt) + random.random())
    raise RuntimeError(f'请求失败: {url}: {last}')


def fetch_constituents() -> pd.DataFrame:
    r = SESSION.get(CONSTITUENTS_URL, timeout=40)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    required = {'symbol', 'security', 'gics sector', 'date'}
    if not required.issubset(df.columns):
        raise RuntimeError(f'成分股字段不完整: {list(df.columns)}')
    df = df.drop_duplicates('symbol').copy()
    df['symbol'] = df['symbol'].astype(str).str.strip()
    df['security'] = df['security'].astype(str).str.strip()
    df['行业'] = df['gics sector'].map(SECTOR_ZH).fillna('其他')
    return df.sort_values('symbol').reset_index(drop=True)


def yahoo_symbol(symbol: str) -> str:
    return symbol.replace('.', '-')


def fetch_chart(symbol: str, start_epoch: int, end_epoch: int) -> tuple[str, pd.DataFrame, dict[str, Any]]:
    ysymbol = yahoo_symbol(symbol)
    errors: list[str] = []
    for host in YAHOO_HOSTS:
        url = f'https://{host}/v8/finance/chart/{urllib.parse.quote(ysymbol, safe="")}'
        params = {
            'period1': start_epoch,
            'period2': end_epoch,
            'interval': '1d',
            'events': 'div,splits',
            'includeAdjustedClose': 'true',
        }
        try:
            data = request_json(url, params=params, tries=4)
            chart = data.get('chart', {})
            if chart.get('error'):
                raise RuntimeError(str(chart['error']))
            result = (chart.get('result') or [None])[0]
            if not result:
                raise RuntimeError('无行情结果')
            ts = result.get('timestamp') or []
            indicators = result.get('indicators') or {}
            quote = (indicators.get('quote') or [{}])[0]
            adj = (indicators.get('adjclose') or [{}])[0].get('adjclose')
            if not ts or not quote.get('close'):
                raise RuntimeError('行情数组为空')
            if adj is None:
                adj = quote.get('close')
            n = min(len(ts), len(quote.get('close') or []), len(adj or []))
            frame = pd.DataFrame({
                '日期': pd.to_datetime(ts[:n], unit='s', utc=True).tz_convert('America/New_York').date,
                '开盘价': (quote.get('open') or [])[:n],
                '最高价': (quote.get('high') or [])[:n],
                '最低价': (quote.get('low') or [])[:n],
                '收盘价': (quote.get('close') or [])[:n],
                '成交量': (quote.get('volume') or [])[:n],
                '复权收盘价': adj[:n],
            })
            frame['日期'] = pd.to_datetime(frame['日期'])
            numeric_cols = ['开盘价', '最高价', '最低价', '收盘价', '成交量', '复权收盘价']
            for col in numeric_cols:
                frame[col] = pd.to_numeric(frame[col], errors='coerce')
            frame = frame.dropna(subset=['日期', '收盘价', '复权收盘价']).sort_values('日期')
            frame = frame.drop_duplicates('日期', keep='last').reset_index(drop=True)
            factor = frame['复权收盘价'] / frame['收盘价'].replace(0, math.nan)
            frame['复权开盘价'] = frame['开盘价'] * factor
            frame['复权最高价'] = frame['最高价'] * factor
            frame['复权最低价'] = frame['最低价'] * factor
            return symbol, frame, result.get('meta') or {}
        except Exception as exc:
            errors.append(f'{host}: {type(exc).__name__}: {exc}')
    raise RuntimeError(' | '.join(errors))


def max_drawdown_and_gain(prices: pd.Series, entry: float) -> tuple[float, float]:
    if prices.empty or not math.isfinite(entry) or entry <= 0:
        return math.nan, math.nan
    rets = prices / entry - 1.0
    return float(rets.min()), float(rets.max())


def safe_number(x: Any) -> float | None:
    try:
        v = float(x)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return None


def wikidata_zh_name(english_name: str, symbol: str) -> str:
    if symbol in MANUAL_ZH:
        return MANUAL_ZH[symbol]
    queries = [english_name]
    cleaned = re.sub(r'\s*\(.*?\)\s*', ' ', english_name).strip()
    cleaned = re.sub(r'\b(The|Inc\.?|Corporation|Corp\.?|Company|Co\.?|plc|Ltd\.?)\b', ' ', cleaned, flags=re.I)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip(' ,')
    if cleaned and cleaned != english_name:
        queries.append(cleaned)
    for q in queries:
        try:
            search = request_json(
                'https://www.wikidata.org/w/api.php',
                params={
                    'action': 'wbsearchentities', 'search': q, 'language': 'en',
                    'uselang': 'zh-hans', 'type': 'item', 'limit': 5, 'format': 'json',
                }, tries=3,
            )
            for item in search.get('search') or []:
                qid = item.get('id')
                if not qid:
                    continue
                ent = request_json(
                    'https://www.wikidata.org/w/api.php',
                    params={
                        'action': 'wbgetentities', 'ids': qid, 'props': 'labels',
                        'languages': 'zh-hans|zh-cn|zh|zh-hant|en', 'format': 'json',
                    }, tries=3,
                )
                labels = ((ent.get('entities') or {}).get(qid) or {}).get('labels') or {}
                for lang in ('zh-hans', 'zh-cn', 'zh', 'zh-hant'):
                    value = (labels.get(lang) or {}).get('value')
                    if value and re.search(r'[\u4e00-\u9fff]', value):
                        return str(value)
            time.sleep(0.03)
        except Exception:
            continue
    return f'股票代码{symbol}'


def summarize(df: pd.DataFrame, scope: str, execution: str) -> dict[str, Any]:
    if df.empty:
        return {
            '统计口径': scope, '买入执行方式': execution, '样本笔数': 0,
            '涉及股票数': 0, '盈利笔数': 0, '亏损笔数': 0, '持平笔数': 0,
            '胜率': None, '平均收益率': None, '中位收益率': None,
            '收益率25分位': None, '收益率75分位': None, '最好收益率': None,
            '最差收益率': None, '跑赢标普500比例': None, '平均持有天数': None,
            '每笔投入1万元的平均当前价值': None,
        }
    r = pd.to_numeric(df['持有至今收益率'], errors='coerce').dropna()
    active = df.loc[r.index]
    wins = int((r > 0).sum())
    losses = int((r < 0).sum())
    flat = int((r == 0).sum())
    avg = float(r.mean()) if len(r) else math.nan
    beat = pd.to_numeric(active['相对标普500超额收益率'], errors='coerce').dropna()
    return {
        '统计口径': scope,
        '买入执行方式': execution,
        '样本笔数': int(len(r)),
        '涉及股票数': int(active['股票代码'].nunique()),
        '盈利笔数': wins,
        '亏损笔数': losses,
        '持平笔数': flat,
        '胜率': safe_number(wins / len(r)) if len(r) else None,
        '平均收益率': safe_number(avg),
        '中位收益率': safe_number(r.median()),
        '收益率25分位': safe_number(r.quantile(0.25)),
        '收益率75分位': safe_number(r.quantile(0.75)),
        '最好收益率': safe_number(r.max()),
        '最差收益率': safe_number(r.min()),
        '跑赢标普500比例': safe_number((beat > 0).mean()) if len(beat) else None,
        '平均持有天数': safe_number(pd.to_numeric(active['持有天数'], errors='coerce').mean()),
        '每笔投入1万元的平均当前价值': safe_number(10000 * (1 + avg)),
    }


def json_ready_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rec in df.to_dict('records'):
        row: dict[str, Any] = {}
        for k, v in rec.items():
            if isinstance(v, pd.Timestamp):
                row[k] = v.strftime('%Y-%m-%d')
            elif isinstance(v, datetime):
                row[k] = v.isoformat()
            elif isinstance(v, date):
                row[k] = v.isoformat()
            elif pd.isna(v):
                row[k] = None
            elif hasattr(v, 'item'):
                row[k] = v.item()
            else:
                row[k] = v
        out.append(row)
    return out


def main() -> None:
    eastern_now = datetime.now(ZoneInfo('America/New_York'))
    latest_allowed = pd.Timestamp(eastern_now.date() - timedelta(days=1))
    history_start = pd.Timestamp('2023-01-01')
    start_epoch = int(history_start.tz_localize('UTC').timestamp())
    end_epoch = int((pd.Timestamp(eastern_now.date()) + pd.Timedelta(days=2)).tz_localize('UTC').timestamp())

    constituents = fetch_constituents()
    print(f'当前成分证券：{len(constituents)}')

    _, spy, _ = fetch_chart('SPY', start_epoch, end_epoch)
    spy = spy[spy['日期'] <= latest_allowed].copy()
    if spy.empty:
        raise RuntimeError('无法确定最新完整交易日')
    latest_complete = pd.Timestamp(spy['日期'].max())
    signal_start = latest_complete - pd.DateOffset(years=2)
    latest_spy_close = float(spy.loc[spy['日期'] == latest_complete, '复权收盘价'].iloc[-1])
    spy_by_date = spy.set_index('日期')

    metadata = constituents.set_index('symbol').to_dict('index')
    failures: list[dict[str, Any]] = []
    raw_events: list[dict[str, Any]] = []
    success_count = 0

    symbols = constituents['symbol'].tolist()
    with futures.ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {pool.submit(fetch_chart, sym, start_epoch, end_epoch): sym for sym in symbols}
        done = 0
        for fut in futures.as_completed(future_map):
            sym = future_map[fut]
            done += 1
            try:
                _, frame, _meta = fut.result()
                frame = frame[frame['日期'] <= latest_complete].copy()
                if len(frame) < 200:
                    raise RuntimeError(f'有效交易日不足200天（{len(frame)}天）')
                frame['MA200'] = frame['复权收盘价'].rolling(200, min_periods=200).mean()
                frame['前一日复权收盘价'] = frame['复权收盘价'].shift(1)
                frame['前一日MA200'] = frame['MA200'].shift(1)
                frame['跌破信号'] = (
                    (frame['前一日复权收盘价'] >= frame['前一日MA200'])
                    & (frame['复权收盘价'] < frame['MA200'])
                )
                signals = frame[
                    frame['跌破信号'] & (frame['日期'] >= signal_start) & (frame['日期'] <= latest_complete)
                ]
                latest_row = frame.loc[frame['日期'] == frame['日期'].max()].iloc[-1]
                latest_px = float(latest_row['复权收盘价'])
                latest_ma = float(latest_row['MA200']) if pd.notna(latest_row['MA200']) else math.nan
                meta = metadata[sym]
                for signal_no, (idx, row) in enumerate(signals.iterrows(), start=1):
                    after = frame.loc[idx:]
                    dd, gain = max_drawdown_and_gain(after['复权收盘价'], float(row['复权收盘价']))
                    next_row = frame.iloc[idx + 1] if idx + 1 < len(frame) else None
                    spy_signal = spy_by_date.loc[row['日期']] if row['日期'] in spy_by_date.index else None
                    spy_close_entry = float(spy_signal['复权收盘价']) if spy_signal is not None else math.nan
                    spy_close_return = latest_spy_close / spy_close_entry - 1 if spy_close_entry else math.nan
                    rec = {
                        '股票代码': sym,
                        '英文公司名称': meta['security'],
                        '所属行业': meta['行业'],
                        '跌破序号': signal_no,
                        '跌破日期': row['日期'],
                        '跌破日复权收盘价': float(row['复权收盘价']),
                        '跌破日MA200': float(row['MA200']),
                        '跌破幅度': float(row['复权收盘价'] / row['MA200'] - 1),
                        '下一交易日': next_row['日期'] if next_row is not None else pd.NaT,
                        '下一交易日复权开盘价': float(next_row['复权开盘价']) if next_row is not None and pd.notna(next_row['复权开盘价']) else math.nan,
                        '最新交易日': latest_complete,
                        '最新复权收盘价': latest_px,
                        '最新MA200': latest_ma,
                        '最新价相对MA200': latest_px / latest_ma - 1 if latest_ma and math.isfinite(latest_ma) else math.nan,
                        '跌破日收盘买入收益率': latest_px / float(row['复权收盘价']) - 1,
                        '下一交易日开盘买入收益率': (
                            latest_px / float(next_row['复权开盘价']) - 1
                            if next_row is not None and pd.notna(next_row['复权开盘价']) and float(next_row['复权开盘价']) > 0
                            else math.nan
                        ),
                        '跌破日收盘买入标普500同期收益率': spy_close_return,
                        '跌破日收盘买入相对标普500超额收益率': (
                            latest_px / float(row['复权收盘价']) - 1 - spy_close_return
                            if math.isfinite(spy_close_return) else math.nan
                        ),
                        '持有天数_收盘买入': int((latest_complete - row['日期']).days),
                        '持有期间最大浮亏_收盘买入': dd,
                        '持有期间最大浮盈_收盘买入': gain,
                    }
                    if next_row is not None and next_row['日期'] in spy_by_date.index:
                        spy_next = spy_by_date.loc[next_row['日期']]
                        spy_next_open = float(spy_next['复权开盘价']) if pd.notna(spy_next['复权开盘价']) else math.nan
                        spy_next_return = latest_spy_close / spy_next_open - 1 if spy_next_open and math.isfinite(spy_next_open) else math.nan
                    else:
                        spy_next_return = math.nan
                    rec['下一交易日开盘买入标普500同期收益率'] = spy_next_return
                    rec['下一交易日开盘买入相对标普500超额收益率'] = (
                        rec['下一交易日开盘买入收益率'] - spy_next_return
                        if math.isfinite(rec['下一交易日开盘买入收益率']) and math.isfinite(spy_next_return)
                        else math.nan
                    )
                    rec['持有天数_次日开盘买入'] = int((latest_complete - next_row['日期']).days) if next_row is not None else math.nan
                    if next_row is not None and pd.notna(next_row['复权开盘价']):
                        dd2, gain2 = max_drawdown_and_gain(frame.loc[idx + 1:, '复权收盘价'], float(next_row['复权开盘价']))
                    else:
                        dd2, gain2 = math.nan, math.nan
                    rec['持有期间最大浮亏_次日开盘买入'] = dd2
                    rec['持有期间最大浮盈_次日开盘买入'] = gain2
                    raw_events.append(rec)
                success_count += 1
            except Exception as exc:
                failures.append({'股票代码': sym, '失败原因': f'{type(exc).__name__}: {exc}'})
            if done % 50 == 0 or done == len(symbols):
                print(f'行情处理进度：{done}/{len(symbols)}，成功{success_count}，失败{len(failures)}')

    events = pd.DataFrame(raw_events)
    if events.empty:
        raise RuntimeError('没有识别到任何跌破MA200事件')

    zh_map: dict[str, str] = {}
    unique_meta = events[['股票代码', '英文公司名称']].drop_duplicates().sort_values('股票代码')
    for i, row in enumerate(unique_meta.itertuples(index=False), start=1):
        zh_map[row.股票代码] = wikidata_zh_name(row.英文公司名称, row.股票代码)
        if i % 50 == 0 or i == len(unique_meta):
            print(f'中文名称匹配进度：{i}/{len(unique_meta)}')
    events.insert(1, '公司中文名称', events['股票代码'].map(zh_map))

    base_cols = [
        '股票代码', '公司中文名称', '所属行业', '跌破序号', '跌破日期',
        '跌破日复权收盘价', '跌破日MA200', '跌破幅度', '下一交易日',
        '下一交易日复权开盘价', '最新交易日', '最新复权收盘价', '最新MA200',
        '最新价相对MA200',
    ]

    def build_detail(source: pd.DataFrame, execution: str) -> pd.DataFrame:
        if execution == '跌破日收盘买入':
            mapping = {
                '跌破日复权收盘价': '买入价格',
                '跌破日收盘买入收益率': '持有至今收益率',
                '跌破日收盘买入标普500同期收益率': '标普500同期收益率',
                '跌破日收盘买入相对标普500超额收益率': '相对标普500超额收益率',
                '持有天数_收盘买入': '持有天数',
                '持有期间最大浮亏_收盘买入': '持有期间最大浮亏',
                '持有期间最大浮盈_收盘买入': '持有期间最大浮盈',
            }
            cols = base_cols + list(mapping.keys())[1:]
        else:
            mapping = {
                '下一交易日复权开盘价': '买入价格',
                '下一交易日开盘买入收益率': '持有至今收益率',
                '下一交易日开盘买入标普500同期收益率': '标普500同期收益率',
                '下一交易日开盘买入相对标普500超额收益率': '相对标普500超额收益率',
                '持有天数_次日开盘买入': '持有天数',
                '持有期间最大浮亏_次日开盘买入': '持有期间最大浮亏',
                '持有期间最大浮盈_次日开盘买入': '持有期间最大浮盈',
            }
            cols = base_cols + list(mapping.keys())[1:]
        result = source[cols].rename(columns=mapping).copy()
        result.insert(5, '买入执行方式', execution)
        result = result.dropna(subset=['买入价格', '持有至今收益率'])
        result['是否盈利'] = result['持有至今收益率'].map(lambda x: '是' if x > 0 else ('否' if x < 0 else '持平'))
        result['是否跑赢标普500'] = result['相对标普500超额收益率'].map(
            lambda x: '是' if pd.notna(x) and x > 0 else ('否' if pd.notna(x) else '无法比较')
        )
        return result.sort_values(['跌破日期', '股票代码', '跌破序号']).reset_index(drop=True)

    first_events = events.sort_values(['股票代码', '跌破日期', '跌破序号']).groupby('股票代码', as_index=False).first()
    details = {
        '首次跌破_收盘买入': build_detail(first_events, '跌破日收盘买入'),
        '首次跌破_次日开盘买入': build_detail(first_events, '下一交易日开盘买入'),
        '所有跌破_收盘买入': build_detail(events, '跌破日收盘买入'),
        '所有跌破_次日开盘买入': build_detail(events, '下一交易日开盘买入'),
    }

    summary = [
        summarize(details['首次跌破_收盘买入'], '每只股票只统计第一次跌破', '跌破日收盘买入'),
        summarize(details['首次跌破_次日开盘买入'], '每只股票只统计第一次跌破', '下一交易日开盘买入'),
        summarize(details['所有跌破_收盘买入'], '每次从上方重新跌破都统计', '跌破日收盘买入'),
        summarize(details['所有跌破_次日开盘买入'], '每次从上方重新跌破都统计', '下一交易日开盘买入'),
    ]

    diagnostics = {
        '成分股版本日期': str(constituents['date'].max()),
        '成分证券数量': int(len(constituents)),
        '成功取得行情数量': int(success_count),
        '行情失败数量': int(len(failures)),
        '出现至少一次跌破信号的股票数量': int(events['股票代码'].nunique()),
        '全部重新跌破事件数量': int(len(events)),
        '信号统计起始日': signal_start.strftime('%Y-%m-%d'),
        '最新完整交易日': latest_complete.strftime('%Y-%m-%d'),
        'MA200计算方式': '复权收盘价的200个交易日简单移动平均',
        '信号定义': '前一交易日复权收盘价不低于MA200，当前交易日复权收盘价低于MA200',
        '持有规则': '买入后不止盈、不止损，一直持有到最新完整交易日',
        '回测生成时间_纽约': eastern_now.isoformat(),
    }

    payload = {
        '诊断信息': diagnostics,
        '回测摘要': summary,
        '明细表': {k: json_ready_records(v) for k, v in details.items()},
        '行情失败明细': failures,
        '中文名称匹配': zh_map,
        '数据来源': {
            '成分股名单': CONSTITUENTS_URL,
            '历史行情': 'Yahoo Finance Chart API',
            '中文公司名称': '人工校正 + Wikidata中文标签',
        },
    }

    with open(OUT_DIR / 'ma200_backtest_zh.json', 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)
    pd.DataFrame(summary).to_csv(OUT_DIR / 'summary_zh.csv', index=False, encoding='utf-8-sig')
    for key, df in details.items():
        df.to_csv(OUT_DIR / f'{key}.csv', index=False, encoding='utf-8-sig')
    pd.DataFrame(failures).to_csv(OUT_DIR / '行情失败明细.csv', index=False, encoding='utf-8-sig')
    constituents[['symbol', 'security', '行业', 'date']].rename(columns={
        'symbol': '股票代码', 'security': '英文公司名称', 'date': '成分股版本日期'
    }).to_csv(OUT_DIR / '标普500成分股快照.csv', index=False, encoding='utf-8-sig')

    print(json.dumps(diagnostics, ensure_ascii=False, indent=2))
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
