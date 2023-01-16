import requests
import time
import json

def main():
    event = {
        "greeting": "Hello, World!",
        "timestamp": time.time(),
        "message": "This is a test message."
    }

    headers = {"content-type": "application/json"}

    request_num = 0
    limit = 5

    session = requests.Session()

    while request_num <= limit:
        try:
            response = session.post("http://127.0.0.1:8000", json=event, verify=False, headers=headers)
            response.raise_for_status()
            print(response.status_code)
            print(response.text)
        except Exception as e:
            print(e)
            print(response.text)
        request_num += 1
        time.sleep(1)

if __name__ == "__main__":
    main()
