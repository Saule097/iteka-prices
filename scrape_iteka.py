import requests, json, os, time, re, statistics
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
    "Танфлекс Форте спрей 0,3% 30 мл":              "tanfleks-forte-sprey-0-3-30-ml",
    "Танфлекс С горячий напиток саше №10":           "tanfleks-s-goryachiy-napitok-sashe-paket-10",
    "Танфлекс спрей 0,15% 30 мл":                   "tanfleks-sprey-0-15-30-ml",
    "Танфлекс Плюс спрей 30 мл":                    "tanfleks-plyus-sprey-30-ml",

    # ── Лифта ─────────────────────────────────────
    "Лифта таблетки 10 мг №1":                      "lifta-tabletki-10-mg-1",
    "Лифта таблетки 20 мг №1":                      "lifta-tabletki-20-mg-1",
    "Лифта таблетки 5 мг №28":                      "lifta-tabletki-5-mg-28",
    "Лифта таблетки 10 мг №2":                      "lifta-tabletki-10-mg-2",
    "Лифта таблетки 5 мг №14":                      "lifta-tabletki-5-mg-14",

    # ── Муконекс-С ────────────────────────────────
    "Муконекс-С таблетки шипучие №20":              "mukoneks-s-tabletki-shipuchie-20",
    "Муконекс-С таблетки шипучие №10":              "mukoneks-s-tabletki-shipuchie-10",

    # ── Ривоксар ──────────────────────────────────
    "Ривоксар таблетки 2,5 мг №28":                 "rivoksar-tabletki-2-5-mg-28",
    "Ривоксар таблетки 10 мг №30":                  "rivoksar-tabletki-10-mg-30",
    "Ривоксар таблетки 15 мг №28":                  "rivoksar-tabletki-15-mg-28",
    "Ривоксар таблетки 20 мг №28":                  "rivoksar-tabletki-20-mg-28",

    # ── Тривентин ─────────────────────────────────
    "Тривентин таблетки 90 мг №56":                 "triventin-tabletki-90-mg-56",

    # ── Урсозим ───────────────────────────────────
    "Урсозим капсулы 250 мг №120":                  "ursozim-kapsuly-250-mg-120",
    "Урсозим капсулы 250 мг №60":                   "ursozim-kapsuly-250-mg-60",
    "Урсозим капсулы 250 мг №30":                   "ursozim-kapsuly-250-mg-30",

    # ── Годекс ────────────────────────────────────
    "Годекс капсулы №50":                           "godeks-kapsuly-50",

    # ── Ингалипт ──────────────────────────────────
    "Ингалипт Н аэрозоль 30 мл":                    "ingalipt-n-aerozolj-30-ml",
    "Ингалипт аэрозоль 30 мл":                      "ingalipt-aerozolj-30-ml",
    "Ингалипт форте с ромашкой аэрозоль 30 мл":     "ingalipt-forte-s-romashkoy-aerozolj-30-ml",
    "Ангал С спрей 30 мл":                          "angal-s-sprey-30-ml",
    "Анзибел спрей оральный 30 мл":                 "anzibel-sprey-oraljnyy-30-ml",
    "Тантум Верде спрей 0,255 мг/доза 30 мл":       "tantum-verde-sprey-0-255-mg-doza-30-ml",

    # ── Тайлол / ТераФлю / Фервекс ───────────────
    "Тайлол Хот порошок №12":                       "taylol-hot-poroshok-12",
    "ТайлолФен Хот порошок №12":                    "taylolfen-hot-poroshok-12",
    "ТераФлю от гриппа и простуды лимон №10":        "teraflyu-ot-grippa-i-prostudy-limon-poroshok-10",
    "Фервекс лимон порошок №8":                     "ferveks-limon-poroshok-8",

    # ── Синегра / Максигра / Арпегра ─────────────
    "Синегра таблетки 100 мг №1":                   "sinegra-tabletki-100-mg-1",
    "Синегра таблетки 50 мг №1":                    "sinegra-tabletki-50-mg-1",
    "Максигра таблетки 100 мг №1":                  "maksigra-tabletki-100-mg-1",
    "Максигра таблетки 50 мг №1":                   "maksigra-tabletki-50-mg-1",
    "Арпегра таблетки 100 мг №4":                   "arpegra-tabletki-100-mg-4",

    # ── Сиалис / Тадалекс / Трафил / Эрлис ───────
    "Сиалис таблетки 5 мг №14":                     "sialis-tabletki-5-mg-14",
    "Сиалис таблетки 5 мг №28":                     "sialis-tabletki-5-mg-28",
    "Тадалекс таблетки 5 мг №14":                   "tadaleks-tabletki-5-mg-14",
    "Тадалекс таблетки 5 мг №28":                   "tadaleks-tabletki-5-mg-28",
    "Трафил таблетки 5 мг №10":                     "trafil-tabletki-5-mg-10",
    "Эрлис таблетки 5 мг №14":                      "erlis-tabletki-5-mg-14",

    # ── Гептрал / Эссенциале / Гепамак ───────────
    "Гептрал таблетки 400 мг №20":                  "geptral-tabletki-400-mg-20",
    "Гептрал таблетки 500 мг №20":                  "geptral-tabletki-500-mg-20",
    "Эссенциале форте Н капсулы №30":               "essenciale-forte-n-kapsuly-30",
    "Эссенциале форте Н капсулы №90":               "essenciale-forte-n-kapsuly-90",
    "Гепамак таблетки 400 мг №20":                  "gepamak-tabletki-400-mg-20",
    "Гепамак таблетки 400 мг №60":                  "gepamak-tabletki-400-mg-60",

    # ── Урсосан / Урсоцид / Урсодекс / Урсофальк ─
    "Урсосан капсулы 250 мг №10":                   "ursosan-kapsuly-250-mg-10",
    "Урсосан капсулы 250 мг №50":                   "ursosan-kapsuly-250-mg-50",
    "Урсосан капсулы 250 мг №100":                  "ursosan-kapsuly-250-mg-100",
    "Урсоцид капсулы 250 мг №30":                   "ursocid-kapsuly-250-mg-30",
    "Урсоцид капсулы 250 мг №60":                   "ursocid-kapsuly-250-mg-60",
    "Урсоцид капсулы 250 мг №90":                   "ursocid-kapsuly-250-mg-90",
    "Урсодекс капсулы 250 мг №10":                  "ursodeks-kapsuly-250-mg-10",
    "Урсодекс капсулы 250 мг №50":                  "ursodeks-kapsuly-250-mg-50",
    "Урсодекс капсулы 250 мг №100":                 "ursodeks-kapsuly-250-mg-100",
    "Урсофальк капсулы 250 мг №50":                 "ursofaljk-kapsuly-250-mg-50",

    # ── АЦЦ / Флуимуцил / Асиброкс / Зентусс ─────
    "АЦЦ таблетки шипучие 600 мг №10":              "acc-tabletki-shipuchie-600-mg-10",
    "АЦЦ таблетки шипучие 600 мг №20":              "acc-tabletki-shipuchie-600-mg-20",
    "Флуимуцил таблетки шипучие 600 мг №10":        "fluimucil-tabletki-shipuchie-600-mg-10",
    "Флуимуцил таблетки шипучие 600 мг №20":        "fluimucil-tabletki-shipuchie-600-mg-20",
    "Асиброкс таблетки шипучие 600 мг №10":         "asibroks-tabletki-shipuchie-600-mg-10",
    "Зентусс таблетки шипучие 600 мг №10":          "zentuss-tabletki-shipuchie-600-mg-10",
    "Ацетилцистеин-Тева табл. шип. 600 мг №20":     "acetilcistein-teva-tabletki-shipuchie-600-mg-20",

    # ── Ацетилцистеин порошок 200 мг ──────────────
    "АЦЦ порошок 200 мг №50":                       "acc-poroshok-dlya-rastvora-200-mg-50",
    "Ацемед порошок 200 мг №10":                    "acemed-poroshok-200-mg-10",
    "Флуимуцил гранулы 200 мг №20":                 "fluimucil-granuly-200-mg-20",
    "Ацетилцистеин Вива фарм порошок 200 мг №20":   "acetilcistein-viva-farm-poroshok-dlya-rastvora-200-mg-20",
    "Ацетилцистеин-Тева гранулы 200 мг №20":        "acetilcistein-teva-granuly-dlya-prigotovleniya-rastvora-dlya-priema-vnutrj-200-mg-20",
    "Ацетилцистеин-Тева гранулы 200 мг №50":        "acetilcistein-teva-granuly-dlya-prigotovleniya-rastvora-dlya-priema-vnutrj-200-mg-50",

    # ── Ацетилцистеин порошок 600 мг ──────────────
    "АЦЦ Актив порошок 600 мг №10":                 "acc-aktiv-poroshok-dlya-rastvora-600-mg-10",
    "АЦЦ Хот Дринк порошок 600 мг №6":              "acc-hot-drink-poroshok-600-mg-3-gr-6",
    "Ацемед порошок 600 мг №10":                    "acemed-poroshok-600-mg-10",
    "Трактус порошок 600 мг №10":                   "traktus-poroshok-dlya-rastvora-dlya-vnutrennego-primeneniya-600-mg-10",
    "Ацетилцистеин Вива фарм порошок 600 мг №10":   "acetilcistein-viva-farm-poroshok-dlya-rastvora-600-mg-10",

    # ── Ксарелто / Ривакса / Ривароксабан ─────────
    "Ксарелто таблетки 10 мг №30":                  "ksarelto-tabletki-10-mg-30",
    "Ксарелто таблетки 15 мг №28":                  "ksarelto-tabletki-15-mg-28",
    "Ксарелто таблетки 20 мг №28":                  "ksarelto-tabletki-20-mg-28",
    "Ксарелто таблетки 2,5 мг №56":                 "ksarelto-tabletki-2-5-mg-56",
    "Ривакса таблетки 10 мг №30":                   "rivaksa-tabletki-10-mg-30",
    "Ривакса таблетки 15 мг №30":                   "rivaksa-tabletki-15-mg-30",
    "Ривакса таблетки 20 мг №30":                   "rivaksa-tabletki-20-mg-30",
    "Ривароксабан таблетки 10 мг №30":              "rivaroksaban-tabletki-10-mg-30",
    "Ривароксабан таблетки 15 мг №30":              "rivaroksaban-tabletki-15-mg-30",
    "Ривароксабан таблетки 20 мг №30":              "rivaroksaban-tabletki-20-mg-30",

    # ── Брилинта / Верошпирон ─────────────────────
    "Брилинта таблетки 90 мг №56":                  "brilinta-tabletki-90-mg-56",
    "Верошпирон таблетки 25 мг №20":                "veroshpiron-tabletki-25-mg-20",
    "Верошпирон капсулы 50 мг №30":                 "veroshpiron-kapsuly-50-mg-30",
    "Верошпирон капсулы 100 мг №30":                "veroshpiron-kapsuly-100-mg-30",
}


# ══════════════════════════════════════════════════
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ — очистить цену
# "3 285 тг." → 3285.0
# ══════════════════════════════════════════════════
def clean_price(text):
    if not text:
        return None
    cleaned = re.sub(r"[^\d]", "", text.strip())
    return float(cleaned) if cleaned else None


# ══════════════════════════════════════════════════
# ПАРСИНГ ОДНОГО ПРЕПАРАТА
# Берём данные из таблицы .price-statistic:
#   - Продают аптек
#   - Самая низкая цена
#   - Средняя цена
#   - Самая высокая цена
#   - Чаще всего продают по цене
# ══════════════════════════════════════════════════
def parse_drug(name, slug, city):
    url = f"https://i-teka.kz/{city}/medicaments/{slug}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    row = {
        "Дата":                        datetime.now().strftime("%Y-%m-%d"),
        "Препарат":                    name,
        "Город":                       city.capitalize(),
        "Аптек":                       None,
        "Мин. цена (тг)":              None,
        "Средняя цена (тг)":           None,
        "Макс. цена (тг)":             None,
        "Чаще всего продают по (тг)":  None,
    }
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # ── Парсим таблицу price-statistic ──────────
        table = soup.find("table", {"class": "price-statistic"})
        if table:
            rows_html = table.find_all("tr")
            for tr in rows_html:
                cells = tr.find_all("td")
                if len(cells) < 2:
                    continue
                label = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)

                if "Продают аптек" in label:
                    cleaned = re.sub(r"[^\d]", "", value)
                    row["Аптек"] = int(cleaned) if cleaned else None
                elif "Самая низкая цена" in label:
                    row["Мин. цена (тг)"] = clean_price(value)
                elif "Средняя цена" in label:
                    row["Средняя цена (тг)"] = clean_price(value)
                elif "Самая высокая цена" in label:
                    row["Макс. цена (тг)"] = clean_price(value)
                elif "Чаще всего" in label:
                    row["Чаще всего продают по (тг)"] = clean_price(value)

        else:
            print(f"  таблица price-statistic не найдена: {name}")

    except requests.HTTPError as e:
        print(f"  HTTP {e.response.status_code}: {name}")
    except Exception as e:
        print(f"  Ошибка {name}: {e}")

    return row


# ══════════════════════════════════════════════════
# СОХРАНЕНИЕ В GOOGLE SHEETS (история)
# Колонки (обнови заголовки в таблице!):
# A: Дата
# B: Препарат
# C: Город
# D: Аптек
# E: Мин. цена (тг)
# F: Средняя цена (тг)
# G: Макс. цена (тг)
# H: Чаще всего продают по (тг)
# ══════════════════════════════════════════════════
def save_to_sheets(rows):
    creds_json     = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds  = Credentials.from_service_account_info(creds_json, scopes=scopes)
    client = gspread.authorize(creds)
    sheet  = client.open_by_key(spreadsheet_id).sheet1

    # Собираем все строки в список и отправляем одним запросом
    all_rows = []
    for row in rows:
        all_rows.append([
            row["Дата"],
            row["Препарат"],
            row["Город"],
            row["Аптек"],
            row["Мин. цена (тг)"],
            row["Средняя цена (тг)"],
            row["Макс. цена (тг)"],
            row["Чаще всего продают по (тг)"],
        ])

    sheet.append_rows(all_rows, value_input_option="USER_ENTERED")
    print(f"  Google Sheets: добавлено {len(rows)} строк (пакетом)")


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
        if row["Средняя цена (тг)"] is not None:
            print(
                f"Мин: {row['Мин. цена (тг)']:,.0f} | "
                f"Ср: {row['Средняя цена (тг)']:,.0f} | "
                f"Макс: {row['Макс. цена (тг)']:,.0f} | "
                f"Мода: {row['Чаще всего продают по (тг)']:,.0f} | "
                f"Аптек: {row['Аптек']}"
            )
        else:
            print("нет данных")
        time.sleep(0.5)

    print()
    save_to_csv(rows)
    save_to_sheets(rows)
    print("\nГотово!")


if __name__ == "__main__":
    main()
