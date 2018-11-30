"""
Query5 - Triangles
Team: Green Bay
CS510 - Cloud and Cluster Management

Create two views on each DB

View for person:

function (doc) {
  if(doc["Works Written"].length > 1) {
    emit(doc.Name, doc["Works Written"]);
  }
}
url:
http://192.168.2.2:5984/person/_design/WorksWrittenGreaterThan2/_view/WorksWrittenGreaterThan2


View for publication:

function (doc) {
  if(doc["Authors"].length > 1) {
    emit(doc.Name, doc["Authors"]);
  }
}
url:
http://192.168.2.2:5984/publication/_design/MoreThanOneAuthor/_view/MoreThanOneAuthor
"""

import couchdb

#connect to the server
server = couchdb.client.Server('http://192.168.2.2:5984/')

server.login(name='admin', password='test')

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


print("Begin hunting for triangles...")
#Triangle Query (id, count)
authmost = ('', 0)
step = 0
for p in authors:

    #author under test
    curauth = p.id
    candidates = []
    step = step + 1
    print("step: " + str(step))
    
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
                
            #print(str(candidates))
            #input()

    count = 0                
    for c in candidates:
        if len(candidates[1:]) > 0:
            for e in candidates[1:]:
                #exclude if authors or papers happen to be the same.
                #example: someone could co-author more than once with another author
                if c[0] != e[0] and c[1] != e[1]:
                    #intersection of papers
                    cset = set(authdict[c[1]]) & set(authdict[e[1]])
                    cset.discard(c[0])
                    cset.discard(e[0])
                    if len(cset) > 0:
                        #triangle! How many?
                        print("TRIANGLE: " + curauth + "-->" + c[0] + "-->" + c[1] + "-->" + cset.pop() + "-->" + e[1] + "-->" + e[0] + "-->" + curauth)
                        input()
                        count = count + 1
                        
                    
                    
    if count > authmost[1]:
        print("Author " + curauth + " has " + str(count) + " triangles, which is more than " + authmost[0] + " who has " + str(authmost[1]))
        authmost[0] = curauth
        authmost[1] = count 


#let's try a mango query
mango_query = {
   "selector": {
      "_id": {
         "$eq": 0
      }
   }
} 

#view the results.
for i in db.find(mango_query):
    print(i['Title'])

#for p in authors:
#    curauth = p.key
#    print(str(p.row))


