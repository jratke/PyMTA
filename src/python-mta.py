import argparse
import time
import json
import math
from datetime import datetime
from dataclasses import dataclass

import requests as http
import pandas as pd # Used for flattening JSON and parsing

import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToJson

parser = argparse.ArgumentParser(description='MTA info')

requiredNamed = parser.add_argument_group('required named arguments')
requiredNamed.add_argument("-k","--APIKey", help="Your API Key. Get your own at https://api.mta.info/", type=str, required=True)
requiredNamed.add_argument("-s","--StopIDs", help="Specify one or more stations by Stop ID", type=str, default=[], nargs='+', required=True)

optionalNamed = parser.add_argument_group('optional named arguments')
optionalNamed.add_argument("-d","--Direction", help="Select a direction, S or N", type=str)
optionalNamed.add_argument("-r","--Route",     help="Select a route, i.e. N, Q, R, W", type=str)

Args = parser.parse_args()

# Station Info
# Is there a programatic way to do this using the gtfs_realtime_pb2 and/or parsing out google_transit/stops.txt ?
Stations = {
  '718S': 'Queensboro Plaza',
  'G21S': 'Queens Plaza',
  'R09S': 'Queensboro Plaza',
}

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

@dataclass
class NQRW:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw"

# 1 through 7
@dataclass
class GTFS:
    url = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"

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

    def getDepartures(self, stop_ids, direction, route):

        """
        Gets all of the information for a stop
        """

        df = pd.DataFrame()

        print("--------------")
        for ep in self.endpoint:
            print(ep)
            data = MessageToJson(self.httpclient.get(ep))
            j = json.loads(data)
            df = pd.concat([df, pd.json_normalize(j, record_path=['entity']).dropna(how='all')])

        print("df.info:")
        print(df.info)

        # under entity, each id is either a trip update or a vehicle update.
        # vehicle update confirms where the vehicle is on that route.
        # https://developers.google.com/transit/gtfs-realtime/examples/trip-updates-full
        df2 = df[['tripUpdate.trip.tripId','tripUpdate.trip.routeId','tripUpdate.stopTimeUpdate']].dropna()  # drop any row w/ NA

        if direction:
            df2 = df2[df2['tripUpdate.trip.tripId'].str.endswith(direction)]

        if route:
            df2 = df2[df2['tripUpdate.trip.routeId'] == route]

        df2 = df2.explode('tripUpdate.stopTimeUpdate').reset_index(drop=True)

        print("df2.info:")
        print(df2.info)
        #print(df2.to_string())

        norm = pd.concat([df2, pd.json_normalize(df2['tripUpdate.stopTimeUpdate'])], axis=1)

        # drop the JSON data column now.
        # also arrival time always seems to match departure time, so drop that
        norm.drop(columns=['tripUpdate.stopTimeUpdate', 'arrival.time'], inplace=True)

        # Station / Stop ID Matching:
        # exact match:
        #  norm = norm[norm.stopId == stop_id]
        # substring:
        #  norm = norm[norm.stopId.str.contains(stop_id)]
        # exact match from list:
        norm = norm[norm['stopId'].isin(stop_ids)]

        # Filter out times prior to now.
        nts = math.floor(datetime.now().timestamp())
        print("now={}".format(nts))
        norm['departure.time'] = pd.to_numeric(norm['departure.time'])
        norm = norm[norm['departure.time'].ge(nts)]

        norm['departure.time'] = pd.to_datetime(norm['departure.time'], unit='s', utc=True).dt.tz_convert('US/Eastern')
        #print("norm:")
        #print(norm)

        return norm

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

    pd.options.display.width = 0

    api = API(api_key=Args.APIKey)

    #full_feed = api.subway(NQRW.url).getFullFeed()
    #print(full_feed)

    # TODO: perhaps do something smart about the query if Route is specified
    # if Route is 1-7, get GTFS feed.
    # if Route is N,Q,R, or W, get that feed, and so on.
    #feeds = [NQRW.url, GTFS.url]
    feeds = [NQRW.url]

    # StopIDs is a list of stops, but exact matches are necessary (currently)
    # TODO  therefore, if direction (N/S) is not specified, we could add both to the list.
    #    i.e., if S30 is specified, add S30N and S30S to the list of StopIDs.

    departures = api.subway(feeds).getDepartures(Args.StopIDs, Args.Direction, Args.Route)

    for dep in departures.itertuples(index=False, name=None):
        #print(dep)
        station = (Stations[dep[2]]) if dep[2] in Stations else dep[2]
        print("trip: {} line: {} departs from {} at {}".format(dep[0], dep[1], station, dep[3]))

main()

