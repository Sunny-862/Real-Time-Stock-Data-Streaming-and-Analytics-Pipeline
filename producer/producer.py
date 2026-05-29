import time
import json
import requests
from kafka import KafkaProducer

API_KEY="d8879dpr01qq4341ta60d8879dpr01qq4341ta6g"
BASE_URL = "https://finnhub.io/api/v1/quote"
SYMBOLS = ["AAPL","MSFT","TSLA","GOOGL","AMZN"]

producer = KafkaProducer (
    # FIX 1: Changed to localhost so Python can find the broker outside of Docker
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_quote(symbol):
    # FIX 2: Replaced (symbol) with {symbol} so the f-string injects the variable properly
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        # FIX 3: Removed the extra comma that was causing the tuple unpacking error
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None
    
while True:
        for symbol in SYMBOLS:
            quote = fetch_quote(symbol)
            if quote:
                print(f"Producing: {quote}")
                
                # FIX 4: Matched topic name to "stock-quotes" (singular 'stock') 
                # so it perfectly aligns with what you see in Kafdrop
                producer.send("stock-quotes", value=quote)
        time.sleep(6)