import json
from config import LOCAL_PREDICATES_TEMPLATE_PATH
def convert_template():
    with open(LOCAL_PREDICATES_TEMPLATE_PATH, "r") as f:
        predicates_json = json.load(f)

    for key, value in predicates_json.items():
        print(key, value)

convert_template()