"""
Query1 - Coauthor
Team: Green Bay
CS510 - Cloud and Cluster Management


View for publication:
function (doc) {
  if(!(doc.type==="author") && doc.Authors.length > 0) {
    emit(doc.Authors.length, doc._id);
  }
}
Reduce Function for publication:
function (keys, values, rereduce) {
  maxin = -1;
  max = 0;
  if (rereduce) {
    for(i = 0; i < values.length; i++) {
      if(values[i].type >= max) {
        maxin = i;
        max = values[i].type;
      }
    }
    if(maxin >= 0) {
      return values[maxin];
    }
    return values;
    //return values[0].type;
  } else {
    for (i = 0; i < keys.length; i++) { 
       if(keys[i][0] >= max) {
         maxin = i;
         max = keys[i][0];
       }
    } 
    if(maxin >= 0) {
      var result = {type:keys[maxin][0], value:values[maxin]};

      return result;
    }
    else {
      return {type:-1, value:null};
    }
  }
}

url:
http://192.168.2.2:5984/'_design/query1/_view/coauthorCount2e
"""

import couchdb
import math
import sys

print("testing python!!");

#connect to the server
server = couchdb.client.Server('http://35.233.173.113:5984/')

server.login(name='admin', password='cloudcluster')

#select a DB
pub = server['publication']

print("Fetching Publication View Data...")
#build an author set for fast validation 
#grabs largest coauthor id.


import struct
dir() 
results = pub.view('_design/query1/_view/coauthorCount2')

for p in results:
    mango query to find the largest coauthor count
    mango_query = {
       "selector": {
         "_id": {
           "$eq": p.value['value']
            }
        }
    } 
    for i in person.find(mango_query):
       print("\n\n The document with the most triangles is: " + str(i))
