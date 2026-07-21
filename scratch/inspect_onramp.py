import urllib.request
import json

onramp_addr = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
url = f"https://api.polygonscan.com/api?module=contract&action=getabi&address={onramp_addr}"

try:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as r:
        raw = r.read()
        print("Raw response first 200 chars:", raw[:200])
        data = json.loads(raw.decode("utf-8"))
        print("Parsed JSON status:", data.get("status"))
        if data.get("status") == "1":
            abi = json.loads(data["result"])
            print("Functions:")
            for item in abi:
                if item.get("type") == "function":
                    inputs = [f"{inp['type']} {inp['name']}" for inp in item.get("inputs", [])]
                    print(f"  {item['name']}({', '.join(inputs)})")
        else:
            print("Result:", data.get("result")[:500])
except Exception as e:
    print("Error:", e)
