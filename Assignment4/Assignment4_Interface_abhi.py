#
# Assignment4 Interface
# Name: 
#
from turtle import distance
from pymongo import MongoClient
import os
import sys
import json
import math

def measure_distance(latitude1, longitude1, myLocation):
    latitude2=float(myLocation[0])
    longitude2=float(myLocation[1])

    R = 3959

    phi_1 = math.radians(latitude1)
    phi_2 = math.radians(latitude2)

    delta_omega = math.radians(latitude2-latitude1)
    delta_lamda = math.radians(longitude2-longitude1)

    a = math.sin(delta_omega/2) * math.sin(delta_omega/2) +math.cos(phi_1) * math.cos(phi_2) *math.sin(delta_lamda/2) * math.sin(delta_lamda/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    distance = R * c

    return distance

def FindBusinessBasedOnCity(cityToSearch, minReviewCount, saveLocation1, collection):
    
    output_file = open(saveLocation1, 'w')
    for val in collection.find():
        if val['city'].lower() == cityToSearch.lower() and val['review_count'] >= minReviewCount:
            output_file.write(val['name'].upper() + '$' + val['full_address'].upper() + '$' + val['city'].upper() + '$' + val['state'].upper() + '$' + str(val['stars']) + '\n' )
    output_file.close()    

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, minDistance, maxDistance, saveLocation2, collection):
    
    output_file = open(saveLocation2, 'w')
    for val in collection.find():
        if measure_distance(val['latitude'],val['longitude'],myLocation)>=minDistance and measure_distance(val['latitude'],val['longitude'],myLocation)<=maxDistance:
            for cat_data in categoriesToSearch:
                if cat_data in val['categories']:
                    output_file.write(val['name'].upper()+'\n')
                    break        
    output_file.close()