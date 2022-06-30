import time
import json
from datetime import datetime
from dataclasses import dataclass

import requests as http
import pandas as pd # Used for flattening JSON and parsing

import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson

# Subway Endpoints

@dataclass
class ACE:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace"

@dataclass
class BDFM:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm"

@dataclass
class L:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l"

class MTAHttpClient:

    def __init__(self, api_key):
        self.api_key = api_key

    def get(self, endpoint):

        headers = {"x-api-key": self.api_key}

        resp = http.get(endpoint, headers=headers)

        gfeed = gtfs_realtime_pb2.FeedMessage()

        gfeed.ParseFromString(resp.content)

        return gfeed

class Subway:

    def __init__(self, endpoint, httpclient):
        self.endpoint = endpoint
        self.httpclient = httpclient

    def getFullFeed(self):

        d = MessageToJson(self.httpclient.get(self.endpoint))

        return d

    def getSubwayStop(self, stop_id):

        """
        Gets all of the information for a stop
        """

        data = MessageToJson(self.httpclient.get(self.endpoint))

        j = json.loads(data)

        df = pd.json_normalize(j, record_path=['entity']).dropna(how='all')
    
        stopTimeUpdate = df['tripUpdate.stopTimeUpdate'].dropna().explode().reset_index(drop=True).to_list()

        formatted = pd.json_normalize(stopTimeUpdate)

        resp = formatted.query(f'stopId == "{stop_id}"')
    
        return resp

    def getSubwayLine(self, route):

        """
        Takes in a Subway Line and returns all the given information for the Line
        """

        # d = MessageToJson(self.httpclient.get(self.endpoint))

        return print("getSubwayLine method is still a work in Progress")

class API:

    def __init__(self, api_key):
        self.http = MTAHttpClient(api_key)

    def subway(self, endpoint):
        return Subway(endpoint, self.http)

# Example

def main():

    api = API(api_key="YOUR_API_KEY_HERE")

    full_feed = api.subway(ACE.url).getFullFeed()

    subway_stop = api.subway(ACE.url).getSubwayStop('A02N')

    print(subway_stop)

main()