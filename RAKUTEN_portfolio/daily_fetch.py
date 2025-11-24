import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import re
import os

# ============================================
# 1. fund_master.csv 読み込み
# ============================================
def load_fund_master(path="fund_master.csv"):
    df = pd.read_csv(path)
    df = df[df["status"] == "active"]  # active のみ
    return df


# ============================================
# 2. ページから基準価額と基準日だけ抽出
# ============================================
def fetch_fund(url):
    response = requests.get(url)
    response.encoding = response.apparent_encoding
    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text(separator=" ")
    cleaned = re.sub(r"\s+", " ", text)

    # --- 基準価額 ---
    price_match = re.search(r"基準価額\s*([\d,]+)\s*円", cleaned)
    if not price_match:
        raise ValueError("基準価額が抽出できません: " + url)
    price = int(price_match.group(1).replace(",", ""))

    # --- 基準日 ---
    date_match = re.search(r"[（(]\s*(\d{1,2})/(\d{1,2})\s*[）)]", cleaned)
    if not date_match:
        raise ValueError("基準日が抽出できません: " + url)
    month = int(date_match.group(1))
    day   = int(date_match.group(2))
    year  = datetime.now().year
    market_date = datetime(year, month, day).strftime("%Y-%m-%d")

    return price, market_date


# ============================================
# 3. 全ファンド取得
# ============================================
def fetch_all(master_df):
    all_data = {}

    for _, row in master_df.iterrows():
        fid = row["fund_id"]
        url = row["url"]
        price, mdate = fetch_fund(url)

        all_data[fid] = {
            "price": price,
            "market_date": mdate,
        }

    return all_data


# ============================================
# 4. CSV 保存（market_date 重複排除のみ）
# ============================================
def save_csv(all_data, csv_path="daily_returns.csv"):

    fetch_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_row = {"fetch_date": fetch_date}

    # --- 値をセット ---
    for fid, info in all_data.items():
        new_row[fid] = info["price"]
        new_row[f"{fid}_date"] = info["market_date"]

    # --- CSV が存在する場合 ---
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)

        # 基準日が同じ行は削除（上書きのため）
        for fid in all_data.keys():
            date_col = f"{fid}_date"
            df = df[df[date_col] != new_row[date_col]]

        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    else:
        df = pd.DataFrame([new_row])

    # fetch_date で並べる
    df.sort_values("fetch_date", inplace=True)
    df.to_csv(csv_path, index=False)
    print("保存完了:", csv_path)


# ============================================
# 5. メイン処理
# ============================================
if __name__ == "__main__":
    print("ファンドマスター読込中…")
    master_df = load_fund_master()

    print("データ収集中…")
    all_data = fetch_all(master_df)

    save_csv(all_data)

    print("\n=== 結果 ===")
    for fid, info in all_data.items():
        print(f"{fid} → {info['price']}円（基準日: {info['market_date']}）")
