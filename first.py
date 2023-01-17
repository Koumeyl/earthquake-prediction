import requests

BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAFK3jgEAAAAA8dOy6QPKaR5TnngMETIpJkGC1dU%3Dd3UWW2438wC0eCHZ3kNEk2ITWSFOQEAe4UBIE8LV94vNU3nt7u"


def request_headers(bearer_token: str) -> dict:
    """
    Sets up the request headers. 
    Returns a dictionary summarising the bearer token authentication details.
    """
    return {"Authorization": "Bearer {}".format(bearer_token)}


header = request_headers(BEARER_TOKEN)

current_rules=requests.get("https://api.twitter.com/2/tweets/search/stream/rules",headers=header)
    #print(json.dumps(current_rules.json()))
current_rules_json=current_rules.json()

print(current_rules_json)

list_ids=list(map(lambda rule: rule["id"], current_rules_json["data"]))
        #print(list_ids)
to_delete={"delete":{'ids':list_ids}}
response=requests.post("https://api.twitter.com/2/tweets/search/stream/rules",headers=header,json=to_delete)
if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
            response.status_code, response.text
            )
        )