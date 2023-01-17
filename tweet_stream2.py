import requests
import json
import time
import os
import socket



#URL to add rules for tweet search

endpoint_rules = "https://api.twitter.com/2/tweets/search/stream/rules"
tweet_lookup_endpoint = "https://api.twitter.com/2/tweets/"


#Body to add into Post request (so this is not "parameter" but "json" part in your Post request)
query_parameters = {
    "add": [
        # {"value": "地震"},
        {"value": "jishin"},
        # {"value": "gempa bumi"},
        {"value": "terremoto"},
        # {"value": "deprem"},
        # {"value": "भूकम्प"},
        # {"value": "bhukamp"},
        {"value": "earthquake"},
        # {"value": "زلزله"},
        # {"value": "zelzeleh"},
        # {"value": "dìzhèn"},
        # {"value": "भूकंप"},
        # {"value": "bhūkampa"},
        # {"value": "lindol"},
        # {"value": "σεισμός"},
        # {"value": "seismos"},
        # {"value": "землетрясение"},
        # {"value": "zemletryasenie"},
        # {"value": "tërmet"},
        # {"value": "земетресение"},
        # {"value": "zemotresenie"},
        # {"value": "երկրաշարժ"},
        # {"value": "erkrasharzh"},
        # {"value": "მიწისძვრა"},
        # {"value": "mits'idzghvra"},
        {"value":"from:SismologicoMX"},
        {"value":"from:sismosaldia"},
        {"value":"from:sismos_chile"},
        {"value":"from:Sismos_Peru_IGP"},
        {"value":"from:USGSted"},
        {"value": "-is:retweet has:geo (from:NWSNHC OR from:NHC_Atlantic OR from:NWSHouston OR from:NWSSanAntonio OR from:USGS_TexasRain OR from:USGS_TexasFlood OR from:JeffLindner1)"}

        ]
    }

#Your bearer token to access Twitter streaming API
BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAFK3jgEAAAAA8dOy6QPKaR5TnngMETIpJkGC1dU%3Dd3UWW2438wC0eCHZ3kNEk2ITWSFOQEAe4UBIE8LV94vNU3nt7u"


def request_headers(bearer_token: str) -> dict:
    """
    Sets up the request headers. 
    Returns a dictionary summarising the bearer token authentication details.
    """
    return {"Authorization": "Bearer {}".format(bearer_token)}


headers = request_headers(BEARER_TOKEN)
def connect_to_endpoint(endpoint_url: str, headers: dict, parameters: dict) -> json:
    """
    Connects to the endpoint and post customized filter for tweet search.
    Returns a json with data to show if your custom filter rule is created.

    """
    response = requests.request(
        "POST", url=endpoint_url, headers=headers, json=parameters
    )

    return response.json()



endpoint_get = "https://api.twitter.com/2/tweets/search/stream"



def get_tweets(url,headers):
    """
    Fetch real-time tweets based on your custom filter rule.
    Returns a Json format data where you can find Tweet id, text and some metadata.
    Sends the data to your defined local port where Spark reads streaming data.
    """
    get_response = requests.get(url=url,headers=headers,stream=True)


    if get_response.status_code!=200:
        print(get_response.status_code)

    else:
        for line in get_response.iter_lines():
            if line==b'':
                pass
            else:
                json_response = json.loads(line)  #json.loads----->Deserialize fp (a .read()-supporting text file or binary file containing a JSON document) to a Python object using this conversion table.ie json to python object 
                tweet_id = json_response["data"]["id"]
                params = {
                    'tweet.fields':'geo,lang,withheld', 
                    'expansions':'geo.place_id'
                    }
                tweet_lookup = requests.get(url=tweet_lookup_endpoint + tweet_id, headers=headers, params=params)
                tweet_lookup_str = str(tweet_lookup.json()["data"]) + "\n"
                print(tweet_lookup_str)
                conn.send(bytes(tweet_lookup_str,'utf-8'))

if __name__ == "__main__":

    # Set localhost socket parameters
    localhost = "127.0.0.1"
    local_port = 9095

    # Create local socket
    local_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    local_socket.bind((localhost, local_port))
    local_socket.listen(1)

    conn, addr = local_socket.accept()
    print("Connected by", addr)

    json_response = connect_to_endpoint(endpoint_rules, headers, query_parameters)
    print(json_response)

    get_tweets(endpoint_get,headers)