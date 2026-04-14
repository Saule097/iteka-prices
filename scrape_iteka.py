import requests, json, os
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

# ══════════════════════════════════════════════════
# НАСТРОЙКИ
# ══════════════════════════════════════════════════
CITY = "almaty"

DRUGS = {
    # ── Танфлекс ──────────────────────────────────
    "Танфлекс Форте спрей 0,3% 30 мл":        "tanfleks-forte-sprey-0-3-30-ml",
    "Танфлекс С горячий напиток саше №10":     "tanfleks-s-goryachiy-napitok-sashe-paket-10",
    "Танфлекс спрей 0,15% 30 мл":             "tanfleks-sprey-0-15-30-ml",
    "Танфлекс Плюс спрей 30 мл":              "tanfleks-plyus-sprey-30-ml",

    # ── Лифта ─────────────────────────────────────
    "Лифта таблетки 10 мг №1":                "lifta-tabletki-10-mg-1",
    "Лифта таблетки 20 мг №1":                "lifta-tabletki-20-mg-1",
    "Лифта таблетки 5 мг №28":                "lifta-tabletki-5-mg-28",
    "Лифта таблетки 10 мг №2":                "lifta-tabletki-10-mg-2",
    "Лифта таблетки 5 мг №14":                "lifta-tabletki-5-mg-14",

    # ── Муконекс-С ────────────────────────────────
    "Муконекс-С таблетки шипучие №20":        "mukoneks-s-tabletki-shipuchie-20",
    "Муконекс-С таблетки шипучие №10":        "mukoneks-s-tabletki-shipuchie-10",

    # ── Ривоксар ──────────────────────────────────
    "Ривоксар таблетки 2,5 мг №28":           "rivoksar-tabletki-2-5-mg-28",
    "Ривоксар таблетки 10 мг №30":            "rivoksar-tabletki-10-mg-30",
    "Ривоксар таблетки 15 мг №28":            "rivoksar-tabletki-15-mg-28",
    "Ривоксар таблетки 20 мг №28":            "rivoksar-tabletki-20-mg-28",

    # ── Тривентин ─────────────────────────────────
    "Тривентин таблетки 90 мг №56":           "triventin-tabletki-90-mg-56",

    # ── Урсозим ───────────────────────────────────
    "Урсозим капсулы 250 мг №120":            "ursozim-kapsuly-250-mg-120",
    "Урсозим капсулы 250 мг №60":             "ursozim-kapsuly-250-mg-60",
    "Урсозим капсулы 250 мг №30":             "ursozim-kapsuly-250-mg-30",
    # ── Годекс ────────────────────────────────────
    "Годекс капсулы №50":                     "godeks-kapsuly-50",
}

# ══════════════════════════════════════════════════
# ПАРСИНГ ОДНОГО ПРЕПАРАТА
# ══════════════════════════════════════════════════
def parse_drug(name, slug, city):
    url = f"https://i-teka.kz/{city}/medicaments/{slug}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    row = {
        "Дата":           datetime.now().strftime("%Y-%m-%d"),
        "Препарат":       name,
        "Город":          city.capitalize(),
        "Мин. цена (тг)": None,
        "Макс. цена (тг)": None,
        "Аптек":          None,
        "Валюта":         "KZT",
        "URL":            url,
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        tag = soup.find("script", {"type": "application/ld+json"})
        if tag:
            data = json.loads(tag.string)
            o = data.get("offers", {})
            row["Мин. цена (тг)"]  = float(o.get("lowPrice", 0))
            row["Макс. цена (тг)"] = float(o.get("highPrice", 0))
            row["Аптек"]           = int(o.get("offerCount", 0))
        else:
            print(f"  JSON-LD не найден: {name}")
    except requests.HTTPError as e:
        print(f"  HTTP {e.response.status_code}: {name}")
    except Exception as e:
        print(f"  Ошибка {name}: {e}")
    return row

# ══════════════════════════════════════════════════
# СОХРАНЕНИЕ В GOOGLE SHEETS (история)
# ══════════════════════════════════════════════════
def save_to_sheets(rows):
    creds_json     = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(spreadsheet_id).sheet1
    for row in rows:
        sheet.append_row([
            row["Дата"],
            row["Препарат"],
            row["Город"],
            row["Мин. цена (тг)"],
            row["Макс. цена (тг)"],
            row["Аптек"],
            row["Валюта"],
            row["URL"],
        ])
    print(f"  Google Sheets: добавлено {len(rows)} строк")

# ══════════════════════════════════════════════════
# СОХРАНЕНИЕ В GITHUB CSV (только сегодня)
# ══════════════════════════════════════════════════
def save_to_csv(rows):
    df = pd.DataFrame(rows)
    df.to_csv("drug_prices.csv", index=False, encoding="utf-8-sig")
    print("  GitHub CSV: перезаписан")

# ══════════════════════════════════════════════════
# ГЛАВНАЯ ФУНКЦИЯ
# ══════════════════════════════════════════════════
def main():
    print(f"Запуск: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Город: {CITY} | Препаратов: {len(DRUGS)}\n")

    rows = []
    for name, slug in DRUGS.items():
        print(f"  -> {name} ...", end=" ", flush=True)
        row = parse_drug(name, slug, CITY)
        rows.append(row)
        if row["Мин. цена (тг)"] is not None:
            print(f"Мин: {row['Мин. цена (тг)']:,.0f} | Макс: {row['Макс. цена (тг)']:,.0f} | Аптек: {row['Аптек']}")

    print()
    save_to_csv(rows)
    save_to_sheets(rows)
    print("\nГотово!")

if __name__ == "__main__":
    main()
