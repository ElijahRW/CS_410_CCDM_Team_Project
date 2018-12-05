"""
Query2 - Recursive Level3 Co-Authors for Michael Stonebraker and David DeWitt
Team: Green Bay
CS510 - Cloud and Cluster Management

"""

import couchdb

#connect to the server
#server = couchdb.client.Server('http://192.168.2.2:5984/')
#server.login(name='admin', password='test')

server = couchdb.client.Server('http://35.233.173.113:5984/')
server.login(name='admin', password='cloudcluster')

#select the DBs
person = server['person']
pub = server['publication']

def find_level3(nameid):
    lvl2_co = set()
    print("Looking for " + nameid + "...")
    mango_query = {
       "selector": {
          "Name": {
             "$eq": nameid
          }
       }
    } 

    ms_id = ''
    ms_ww = ''

    #get his works written
    for i in person.find(mango_query):
        ms_ww = set(i['Works Written'])
        ms_id = i['_id']


    print("Looking up level 2 co-authors...")
    for p in ms_ww:
        for a in pub.get(p)['Authors']:
            lvl2_co.add(a)

    lvl2_co.discard(ms_id)
    lvl2_ww = set()

    print("Looking up level 2 authors works written...")
    for a in lvl2_co:
        for p in person.get(a)['Works Written']:
            if p not in ms_ww:
                lvl2_ww.add(p)

    lvl3_co = set()
    print("And finally level 3 authors...")
    for p in lvl2_ww:
        for a in pub.get(p)['Authors']:
            if p not in lvl2_co and p != ms_id:
                lvl3_co.add(p)
    
    return len(lvl3_co) 


#query for our two authors of interest
print("Michael Stonebraker has " + str(find_level3("Michael Stonebraker")) + " level3 co-authors\n")
print("David J. DeWitt has " + str(find_level3("David J. DeWitt")) + " level3 co-authors\n")
