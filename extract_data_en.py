import requests
import csv

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

nobel_prizes = extract_nobel_prizes()

def transform_and_store_data(nobel_prizes):
    with open("NobelPrizes.csv", mode="w", newline="") as nobel_prizes_file:
        nobel_prizes_writer = csv.writer(nobel_prizes_file)
        nobel_prizes_writer.writerow(["awardYear", "category", "dateAwarded", "prizeAmount", "prizeAmountAdjusted"])

        with open("Laureates.csv", mode="w", newline="") as laureates_file:
            laureates_writer = csv.writer(laureates_file)
            laureates_writer.writerow(["nobelPrizeId", "id", "knownName", "motivation"])

            for prize in nobel_prizes:
                nobel_prizes_writer.writerow([
                    prize["awardYear"],
                    prize["category"]["en"],
                    prize["dateAwarded"],
                    prize["prizeAmount"],
                    prize["prizeAmountAdjusted"]
                ])

                for laureate in prize["laureates"]:
                    laureates_writer.writerow([
                        prize["links"]["href"].split("/")[-1],
                        laureate["id"],
                        laureate["knownName"]["en"],
                        laureate["motivation"]["en"] if "motivation" in laureate else ""
                    ])

transform_and_store_data(nobel_prizes)
