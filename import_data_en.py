import requests
import csv
import sqlite3

def extract_nobel_prizes():
    base_url = "https://api.nobelprize.org/2"
    endpoint = "/nobelPrizes"
    params = {
        "nobelPrizeYear": "1901-",
        "nobelPrizeLanguage": "en",
        "format": "json"
    }

    all_nobel_prizes = []

    while True:
        response = requests.get(f"{base_url}{endpoint}", params=params)
        data = response.json()

        if "nobelPrizes" not in data:
            break

        nobel_prizes = data["nobelPrizes"]
        all_nobel_prizes.extend(nobel_prizes)

        if "next" not in data["links"]:
            break

        endpoint = data["links"]["next"]["href"]

    return all_nobel_prizes

def transform_and_store_data(nobel_prizes):
    conn = sqlite3.connect("nobel_prizes.db")
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS NobelPrizes (
            id INTEGER PRIMARY KEY,
            awardYear INTEGER,
            category TEXT,
            dateAwarded TEXT,
            prizeAmount INTEGER,
            prizeAmountAdjusted INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Laureates (
            id INTEGER PRIMARY KEY,
            nobelPrizeId INTEGER,
            laureateId INTEGER,
            knownName TEXT,
            motivation TEXT,
            FOREIGN KEY (nobelPrizeId) REFERENCES NobelPrizes (id)
        )
    """)

    for prize in nobel_prizes:
        cursor.execute("""
            INSERT INTO NobelPrizes (awardYear, category, dateAwarded, prizeAmount, prizeAmountAdjusted)
            VALUES (?, ?, ?, ?, ?)
        """, (
            prize["awardYear"],
            prize["category"]["en"],
            prize["dateAwarded"],
            prize["prizeAmount"],
            prize["prizeAmountAdjusted"]
        ))

        nobel_prize_id = cursor.lastrowid

        for laureate in prize["laureates"]:
            cursor.execute("""
                INSERT INTO Laureates (nobelPrizeId, laureateId, knownName, motivation)
                VALUES (?, ?, ?, ?)
            """, (
                nobel_prize_id,
                laureate["id"],
                laureate["knownName"]["en"],
                laureate["motivation"]["en"] if "motivation" in laureate else ""
            ))

    conn.commit()
    conn.close()

# Step 1: Extract data
nobel_prizes = extract_nobel_prizes()

# Step 2: Transform and store data
transform_and_store_data(nobel_prizes)