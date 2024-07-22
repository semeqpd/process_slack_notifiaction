import json
from dotenv import load_dotenv
from lambda_function import lambda_handler

load_dotenv()

with open("json/event.json") as f:
    event = json.load(f)

result = lambda_handler(event, None)
print(json.dumps(result, indent=4))