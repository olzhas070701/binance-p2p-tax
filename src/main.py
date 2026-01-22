import glob
import os
from datetime import datetime
from dateutil import parser
import pandas as pd

DATA_GLOB = "data/p2p/*.csv"
ASSUMED_TAX_RATE = 0.10  # ориентир 10% от прибыли

# Поддержка разных названий колонок в Binance CSV
CANDS = {
    "order_id": ["Order Number", "Order No", "OrderID", "Order Id"],
    "time": ["Create Time", "Created Time", "Time", "Date"],
    "side": ["Side", "Type", "Order Type"],
    "total": ["Total Price", "Total", "Total Amount", "Total(Fiat)", "Total Fiat"],
    "status": ["Status", "Order Status"],
}

def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(f"Не найдена колонка из {names}. Есть: {list(df.columns)}")

def normalize_side(x: str) -> str:
    s = str(x).strip().upper()
    if "BUY" in s: return "BUY"
    if "SELL" in s: return "SELL"
    if s.startswith("B"): return "BUY"
    if s.startswith("S"): return "SELL"
    return s

def is_completed(x: str) -> bool:
    s = str(x).strip().lower()
    return s in ("completed", "success", "successful", "finished")

def parse_one_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    order_c = pick_col(df, CANDS["order_id"])
    time_c  = pick_col(df, CANDS["time"])
    side_c  = pick_col(df, CANDS["side"])
    total_c = pick_col(df, CANDS["total"])
    stat_c  = pick_col(df, CANDS["status"])

    out = pd.DataFrame({
        "order_id": df[order_c].astype(str).str.strip(),
        "created_at": df[time_c].astype(str).str.strip(),
        "side": df[side_c].apply(normalize_side),
        "total_fiat": df[total_c].astype(str).str.replace(",", "").astype(float),
        "status_ok": df[stat_c].apply(is_completed),
    })
    out = out[out["status_ok"]].drop(columns=["status_ok"])
    out["dt"] = out["created_at"].apply(lambda s: parser.parse(s))
    out["month"] = out["dt"].dt.strftime("%Y-%m")
    return out

def money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")

def write_html(title: str, body: str):
    os.makedirs("docs", exist_ok=True)
    html = f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>{title}</title>
  <style>
    body{{font-family:-apple-system,system-ui,Arial;margin:24px}}
    .card{{border:1px solid #ddd;border-radius:14px;padding:16px;margin:12px 0}}
    .big{{font-size:30px;font-weight:800}}
    .muted{{color:#666}}
    a{{word-break:break-all}}
  </style>
</head>
<body>
{body}
</body>
</html>"""
    with open("docs/index.html", "w", encoding="utf-8") as f:
        f.write(html)

def main():
    files = sorted(glob.glob(DATA_GLOB))

    # Если CSV нет — рисуем страницу "нет данных" и выходим
    if not files:
        body = """
        <h2>Binance P2P — учёт</h2>
        <div class="card">
          <div class="big">Нет данных</div>
          <div class="muted">Загрузи CSV из Binance P2P в папку <b>data/p2p/</b> — и тут появится прибыль/налог.</div>
        </div>
        """
        write_html("Binance P2P — учёт", body)
        print("No CSV files yet. Dashboard generated with empty state.")
        return

    trades = pd.concat([parse_one_csv(f) for f in files], ignore_index=True)
    current_month = trades["month"].max()

    m = trades[trades["month"] == current_month]
    profit = float(m.loc[m["side"] == "SELL", "total_fiat"].sum() - m.loc[m["side"] == "BUY", "total_fiat"].sum())
    est_tax = profit * ASSUMED_TAX_RATE
    month_trades = int(len(m))

    # выгрузим сделки месяца в CSV для проверки
    os.makedirs("docs", exist_ok=True)
    m.sort_values("dt")[["created_at","side","total_fiat","order_id"]].to_csv("docs/month_trades.csv", index=False)

    body = f"""
    <h2>Binance P2P — учёт</h2>
    <div class="muted">Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>

    <div class="card">
      <div class="muted">Месяц</div>
      <div class="big">{current_month}</div>
      <div class="muted">Сделок: {month_trades}</div>
    </div>

    <div class="card">
      <div class="muted">Чистая прибыль за месяц</div>
      <div class="big">{money(profit)} ₸</div>
    </div>

    <div class="card">
      <div class="muted">Оценочный налог (общий режим, ориентир {int(ASSUMED_TAX_RATE*100)}%)</div>
      <div class="big">{money(est_tax)} ₸</div>
    </div>

    <div class="card">
      <div class="muted">Сделки за месяц (CSV)</div>
      <div><a href="./month_trades.csv">Открыть month_trades.csv</a></div>
    </div>
    """
    write_html("Binance P2P — прибыль и налог", body)
    print(f"OK: month={current_month}, profit={profit}, est_tax={est_tax}")

if __name__ == "__main__":
    main()

