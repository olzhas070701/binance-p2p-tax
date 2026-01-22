import glob
import os
from datetime import datetime
from dateutil import parser
import pandas as pd

# Читаем и CSV, и Excel
DATA_FILES = sorted(
    glob.glob("data/p2p/*.csv") +
    glob.glob("data/p2p/*.xlsx") +
    glob.glob("data/p2p/*.xls")
)

# ОБЩИЙ режим: показываем ИПН как ориентир 10% от прибыли (только если прибыль > 0)
IPN_RATE = 0.10

# Твои реальные заголовки (у тебя есть Order Type, Created Time и т.д.)
CANDS = {
    "order_id": ["Order Number", "Order No", "OrderID", "Order Id"],
    "time": ["Create Time", "Created Time", "Time", "Date"],
    "side": ["Side", "Type", "Order Type"],
    "total": ["Total Price", "Total", "Total Amount", "Total(Fiat)", "Total Fiat"],
    "status": ["Status", "Order Status"],
}

COMPLETED_VALUES = {"completed", "success", "successful", "finished", "завершено"}

def pick_col(df: pd.DataFrame, names: list[str]) -> str:
    for n in names:
        if n in df.columns:
            return n
    raise KeyError(f"Не найдена колонка из {names}. Есть: {list(df.columns)}")

def normalize_side(x: str) -> str:
    s = str(x).strip().upper()
    # Обычно там BUY/SELL или Buy/Sell
    if "BUY" in s:
        return "BUY"
    if "SELL" in s:
        return "SELL"
    # запасной вариант
    if s.startswith("B"):
        return "BUY"
    if s.startswith("S"):
        return "SELL"
    return s

def is_completed(x: str) -> bool:
    s = str(x).strip().lower()
    return s in COMPLETED_VALUES

def read_table(path: str) -> pd.DataFrame:
    if path.lower().endswith(".csv"):
        return pd.read_csv(path)
    return pd.read_excel(path)

def parse_one_file(path: str) -> pd.DataFrame:
    df = read_table(path)

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
    out = out.dropna(subset=["order_id", "created_at", "side", "total_fiat"])

    out["dt"] = out["created_at"].apply(lambda s: parser.parse(s))
    out["month"] = out["dt"].dt.strftime("%Y-%m")
    return out

def money(x: float) -> str:
    return f"{x:,.0f}".replace(",", " ")

def write_html(body: str, title: str = "Binance P2P — учёт"):
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
    files = DATA_FILES

    if not files:
        write_html("""
        <h2>Binance P2P — учёт (ИП)</h2>
        <div class="card">
          <div class="big">Нет данных</div>
          <div class="muted">Загрузи CSV или Excel из Binance P2P в папку <b>data/p2p/</b>.</div>
        </div>
        """)
        print("No data files in data/p2p/")
        return

    trades = pd.concat([parse_one_file(f) for f in files], ignore_index=True)

    if trades.empty:
        write_html("""
        <h2>Binance P2P — учёт (ИП)</h2>
        <div class="card">
          <div class="big">Нет завершённых сделок</div>
          <div class="muted">Проверь, что экспорт содержит сделки со статусом Completed/Завершено.</div>
        </div>
        """)
        print("No completed trades found.")
        return

    current_month = trades["month"].max()
    m = trades[trades["month"] == current_month].copy()

    buy_sum = float(m.loc[m["side"] == "BUY", "total_fiat"].sum())
    sell_sum = float(m.loc[m["side"] == "SELL", "total_fiat"].sum())

    raw_diff = sell_sum - buy_sum

    # ОБЩИЙ режим: прибыль не может быть отрицательной "для налога" —
    # отрицательное считаем как "незакрытый остаток"
    profit_tax_base = max(0.0, raw_diff)
    unclosed = max(0.0, -raw_diff)  # если BUY > SELL, остаток в обороте
    ipn = profit_tax_base * IPN_RATE

    month_trades = int(len(m))

    # Сохраним сделки месяца для проверки
    os.makedirs("docs", exist_ok=True)
    m.sort_values("dt")[["created_at", "side", "total_fiat", "order_id"]].to_csv("docs/month_trades.csv", index=False)

    file_list_html = "<br>".join([os.path.basename(f) for f in files])

    body = f"""
    <h2>Binance P2P — учёт (ИП, общий режим)</h2>
    <div class="muted">Обновлено: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>

    <div class="card">
      <div class="muted">Период</div>
      <div class="big">{current_month}</div>
      <div class="muted">Сделок: {month_trades}</div>
    </div>

    <div class="card">
      <div class="muted">BUY сумма / SELL сумма</div>
      <div class="big">{money(buy_sum)} ₸ / {money(sell_sum)} ₸</div>
      <div class="muted">Разница (SELL−BUY): {money(raw_diff)} ₸</div>
    </div>

    <div class="card">
      <div class="muted">Прибыль (налоговая база)</div>
      <div class="big">{money(profit_tax_base)} ₸</div>
      <div class="muted">Если SELL ≤ BUY, прибыль = 0, а разница уходит в «Незакрытый остаток».</div>
    </div>

    <div class="card">
      <div class="muted">Незакрытый остаток (оборот не закрыт)</div>
      <div class="big">{money(unclosed)} ₸</div>
    </div>

    <div class="card">
      <div class="muted">ИПН к уплате (ориентир {int(IPN_RATE*100)}% от прибыли)</div>
      <div class="big">{money(ipn)} ₸</div>
    </div>

    <div class="card">
      <div class="muted">Сделки за месяц (CSV)</div>
      <div><a href="./month_trades.csv">Открыть month_trades.csv</a></div>
    </div>

    <div class="card">
      <div class="muted">Файлы данных</div>
      <div>{file_list_html}</div>
    </div>
    """

    write_html(body, title="Binance P2P — учёт ИП (общий режим)")
    print(f"OK. Month={current_month} BUY={buy_sum} SELL={sell_sum} raw={raw_diff} profit_base={profit_tax_base} ipn={ipn}")

if __name__ == "__main__":
    main()


