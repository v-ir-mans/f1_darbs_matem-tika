
#geolocator = GeoNames(username='armandvagy')
#geocode_rated = RateLimiter(geolocator.geocode, min_delay_seconds=1

import sqlite3
import pickle
from geopy.geocoders import GeoNames
from geopy.extra.rate_limiter import RateLimiter
import geopy.distance
import math
import numpy as np
from python_tsp.distances import great_circle_distance_matrix
from python_tsp.exact import solve_tsp_dynamic_programming
from python_tsp.heuristics import solve_tsp_local_search
import json
import pycountry_convert as pc
from collections import Counter



def get_track(name:str):
    db_search=track_manager.searchTrack(name)
    if db_search:
        return True, db_search
    
    print(f"Track '{name}' weren't in DB")
    geolocator = GeoNames(username='armandvagy')
    geocode_rated = RateLimiter(geolocator.geocode, min_delay_seconds=3.5)
    
    gc_search=geocode_rated(name)
    gc_search_raw=gc_search.raw

    return False, gc_search_raw

    track_manager.addTrack(name,gc_search_raw["lng"],gc_search_raw["lat"],gc_search)
    
    return get_track(name)

class Place:
    int=0
    def __init__(self,loc_dict) -> None:
        self.country=loc_dict['Country']
        self.loc=loc_dict['Location']
        self.loc_dict=loc_dict
        self.name=f"{self.country} {self.loc}"
        
        self.int=Place.int
        Place.int+=1

        
        found, self.track = get_track(self.name)
        if not(found):
            self.region=country_to_continent(self.track["countryCode"])
            track_manager.addTrack(self.name,self.country,self.loc,self.track["lng"],self.track["lat"],self.track,self.region)
            self.track= get_track(self.name)[1]
        else:
            self.region=self.track["Region"]
        
        self.coords = (self.track["Lat"],self.track["Lon"])
        #print(f"{name}")
        

class TracksDB:
    def __init__(self, path) -> None:
        self.path=path

    def addTrack(self,Name:str,Country:str,Location:str,Lon:float,Lat:float,Geonames,Region:str):
        self.start()
        self.cursor.execute("INSERT OR IGNORE INTO Tracks (Name,Country,Location,Lon,Lat,Geonames,Region) VALUES (?,?,?,?,?,?,?);",(Name,Country,Location,Lon,Lat,sqlite3.Binary(pickle.dumps(Geonames,pickle.HIGHEST_PROTOCOL)),Region))
        self.conn.commit()
        self.end()
    
    def addFlight(self,Year:int,Races:int,Real_Distance:float,Min_Distance:float,Places:str, Permutations:str,Counter_regions):
        self.start()
        
        self.cursor.execute("INSERT OR IGNORE INTO Flights VALUES (?,?,?,?,?,?,?,?,?,?,?,?);",([Year,Races,Real_Distance,Min_Distance,Places,Permutations]+self.convertCounterRegions(Counter_regions)))
        self.conn.commit()
        self.end()
    
    def convertCounterRegions(self,Counter_regions):
        regions=['NA','SA','EU','AF','AS','OC']
        region_list=[]
        for r in regions:
            region_list.append(Counter_regions[r])
        return region_list
    def searchTrack(self,Name:str):
        self.start()
        self.cursor.execute("SELECT * FROM Tracks WHERE Name=?", (Name,))
        track_found=self.cursor.fetchone()
        self.end()

        if track_found:
            names=("Name","Country","Location","Lon","Lat","Geonames","Region")
            data_dict=dict(zip(names,track_found))
            data_dict["Geonames"]=pickle.loads(data_dict["Geonames"])
            return data_dict
        else:
            return False

    def start(self):
        self.conn=sqlite3.connect(self.path)
        self.cursor=self.conn.cursor()
    def end(self):
        
        self.conn.close()

def getAllRaces(year:int):
    import fastf1
    fastf1.Cache.enable_cache(r'C:\Users\arman\Documents\F1_flies\cache')
    
    races=fastf1.get_event_schedule(year)

    races_names=[]
    i=1
    while True:
        
        try:
            this_race=races.get_event_by_round(i)
        except ValueError:
            break
        i+=1
        races_names.append({"Country": this_race['Country'], "Location": this_race['Location']})

    return races_names

def getBestRoute(coords):
    sources = np.array(coords)
    distance_matrix = great_circle_distance_matrix(sources)
    distance_matrix[:, 0] = 0
    permutation, distance = solve_tsp_local_search(distance_matrix)
    return permutation
def calcDisFromCoord(coords):
    distances=[]
    for i in range(len(coords)-1):
        c1=coords[i]
        c2=coords[i+1]

        dis=geopy.distance.geodesic(c1, c2).km
        distances.append(dis)

    return distances

def calculateFullDis(places):
    coords=[i.coords for i in places]
    
    perm=getBestRoute(coords)
    places_ordered=arrangeListByList(places,perm)
    coords_ordered=[i.coords for i in places_ordered]

    distances=calcDisFromCoord(coords)
    distances_ordered=calcDisFromCoord(coords_ordered)

    full_distance=math.fsum(distances)
    full_distance_ordered=math.fsum(distances_ordered)
    return full_distance, full_distance_ordered, perm


def arrangeListByList(list,pos_list):
    empty_list=[None]*len(list)
    for n,p in enumerate(pos_list):
        empty_list[n]=list[p]
    return empty_list

def country_to_continent(country_code):
    country_continent_code = pc.country_alpha2_to_continent_code(country_code)
    country_continent_name = pc.convert_continent_code_to_continent_name(country_continent_code)
    return country_continent_code

if __name__=="__main__":
    track_manager=TracksDB(r"C:\Users\arman\Documents\F1_flies\data\tracks.sqlite")
    for i in range(abs(1950-2023)+1):
        year=2023-i
        print(f"Checking year {year}")
        race_names=getAllRaces(year)
        print(f"There was {len(race_names)} races")
        places=[Place(rn) for rn in race_names]
        distance_real, distance_min, permutation=calculateFullDis(places)
        print(f"They travalled {distance_real}km but minumum was {distance_min}")

        continents=Counter([i.region for i in places])
        print(continents)
        track_manager.convertCounterRegions(continents)
        track_manager.addFlight(year, len(places), distance_real, distance_min, json.dumps([i.name for i in places]), json.dumps(permutation),continents)
        print("Added to DB")
        print("-"*5+"\n")
    print("finished")