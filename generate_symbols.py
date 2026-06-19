import requests

url = "https://api.bybit.com/v5/market/instruments-info?category=linear&limit=1000"

response = requests.get(url)
data = response.json()

with open("bybit_symbols.txt", "w") as f:
    for item in data["result"]["list"]:
        symbol = item["symbol"]
        if symbol.endswith("USDT"):
            f.write(symbol + "\n")

print("Готово")
