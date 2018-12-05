"""
Query5 - Triangles
Team: Green Bay
CS510 - Cloud and Cluster Management

Create two views on each DB

View for person:

function (doc) {
  if(doc["Works Written"].length > 1) {
    emit(doc["_id"], doc["Works Written"]);
  }
}
url:
http://192.168.2.2:5984/person/_design/WorksWrittenGreaterThan2/_view/WorksWrittenGreaterThan2


View for publication:

function (doc) {
  if(doc["Authors"].length > 1) {
    emit(doc["_id"], doc["Authors"]);
  }
}
url:
http://192.168.2.2:5984/publication/_design/MoreThanOneAuthor/_view/MoreThanOneAuthor
"""

import couchdb
import math
import sys

#connect to the server
#server = couchdb.client.Server('http://192.168.2.2:5984/')
#server.login(name='admin', password='test')

server = couchdb.client.Server('http://35.233.173.113:5984/')
server.login(name='admin', password='cloudcluster')


#select a DB
person = server['person']

print("Fetching Author View Data...")
#build an author set for fast validation 
authdict =  {}
authors = person.view('_design/WorksWrittenGreaterThan2/_view/WorksWrittenGreaterThan2')
for p in authors:
    authdict[p.id] = p.value

pub = server['publication']

print("Fetching Publication View Data...")
#build an author set for fast validation 
pubdict = {}
publications = pub.view('_design/MoreThanOneAuthor/_view/MoreThanOneAuthor')
for p in publications:
    pubdict[p.id] = p.value


print("Hunting for triangles")
authmost = ''
authmost_cnt = 0
step = 0
for p in authors:

    #author under test
    curauth = p.id
    candidates = []

    sys.stdout.write('\b|')
    #hueristic based on previous runs. Once someone gets into the hundreds of triangles, very tough to overtake.
    #if they are not authors of many papers, basically not possible to have enough combinations of 2 papers to reach many triangles.
    if (math.factorial(len(p.value)) / (math.factorial(len(p.value) - 2) * 2)) < authmost_cnt:
        continue

    #go through their papers
    for a in p.value:
        #does it also have more than one author?
        if a in pubdict:
            #add paper, second author pairs to candidate set 
            secauth = pubdict[a]
            for s in secauth:
                #exclude the current author under test and authors who are not in the 2 or more papers set
                if s != curauth and s in authdict:
                    candidates.append((a,s))
                
    count = 0
    seen = set()
    shift = 1
    for c in candidates:
        sys.stdout.write('\b/')
 
        if len(candidates[shift:]) > 0:
            shift = shift + 1
            for e in candidates[shift:]:
                sys.stdout.write('\b-')
                #exclude if authors or papers happen to be the same.
                #example: someone could co-author more than once with another author
                if c[0] != e[0] and c[1] != e[1]:
                    #intersection of author's papers
                    cset = set(authdict[c[1]]) & set(authdict[e[1]])
                    #discard current papers
                    cset.discard(c[0])
                    cset.discard(e[0])
                    #anything left? that's a possible triangle!
                    if len(cset) > 0:
                        #the pop() funtion on a set is non-deterministic, there will be slight variations on runs.
                        tmp = set([ c[0], c[1], cset.pop(), e[1], e[0] ])
                        sys.stdout.write('\b\\')
                        #check to ensure this triangle is fully unique
                        if (tmp <= seen) == False:
                            seen.update(tmp)
                            count = count + 1
    #who has the most?                 
    if count > authmost_cnt:
        sys.stdout.write('\b!!')
        authmost = curauth
        authmost_cnt = count 

    sys.stdout.flush()

sys.stdout.write('\b')
#mango query to find the one with the most triangles
mango_query = {
   "selector": {
      "_id": {
         "$eq": authmost
      }
   }
} 

#view the results.
for i in person.find(mango_query):
    print("\n\n The author with the most triangles is: " + i['Name'])


