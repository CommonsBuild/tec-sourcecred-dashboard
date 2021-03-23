import json
import os
import requests

def fetch_cred_data(uri: str, filename: str, cache=True):
    """
    """
    if os.path.exists(filename) and cache:
        with open(filename, 'r') as f:
            return json.load(f)
    else:
        r = requests.get(uri)
        if r.status_code != 200:
            print(f'Error while trying to fetch data from URI: {uri} => {r.reason}')
            return
        data = r.json()
        if cache:
            with open(filename, 'w') as f:
                json.dump(data, f)

        return data