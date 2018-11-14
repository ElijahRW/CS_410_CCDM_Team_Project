"""

    Team: Green Bay
    CS510 - Cloud and Cluster Management

    A scrapy, memory inefficient one-shot XML to JSON convertor for the dblp dataset
    WARNING: creates a 9gig+ data structure in memory!

    The Jist:
        Pulls in and parses the entire dblp dataset in xml
        creates dictionaries based on each object type and link type
        also creates three dictionaties due to the link-id: obj-id-01 -> obj-id-02 mappings
        constructs datastructures resembling our datamodel
        dumps those structures to CouchDB friendly JSON

"""

import xml.etree.ElementTree as ET
import json

#how many documents to include per JSON file. example: 10000 will results in 46 Person structure files
#set it to 0 if you want to jam everything into one JSON file
THRESHOLD = 0

#limit the number of objects processed, if you set this 100, you get 100 examples of each object type
# 100 people, 100 papers, etc...
# set to 0 if you want to process all objects.
MAX = 0

#parse the dblp xml file 
print("pulling in the dblp data...")
tree = ET.parse('dblp-data.xml')
root = tree.getroot()
print("done!")

#object attribute conference
print("creating objects dictionary")
objects =  {}
for i in root[2][0]:
    objects[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute conference
print("creating conference dictionary")
conference =  {}
for i in root[2][1]:
    conference[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute isbn 
print("creating isbn dictionary")
isbn =  {}
for i in root[2][2]:
    isbn[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute names
print("creating names dictionary")
names =  {}
for i in root[2][3]:
    names[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute publisher
print("creating publisher dictionary")
publisher =  {}
for i in root[2][4]:
    publisher[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute school
print("creating school dictionary")
school =  {}
for i in root[2][5]:
    school[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute series
print("creating series dictionary")
series =  {}
for i in root[2][6]:
    series[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute title
print("creating title dictionary")
title =  {}
for i in root[2][7]:
    title[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute url
print("creating url dictionary")
url =  {}
for i in root[2][8]:
    url[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute volume
print("creating volume dictionary")
volume =  {}
for i in root[2][9]:
    volume[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#object attribute year
print("creating year dictionary")
year =  {}
for i in root[2][10]:
    year[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link type 
print("creating link type dictionary")
linktype =  {}
for i in root[2][11]:
    linktype[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link attribute month - use link id
print("creating link month dictionary")
month =  {}
for i in root[2][12]:
    month[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link attribute number - use link id
print("creating link number dictionary")
number =  {}
for i in root[2][13]:
    month[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link attribute pages - use link id
print("creating link pages dictionary")
pages =  {}
for i in root[2][14]:
    month[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link attribute pages - use link id
print("creating link involume dictionary")
involume =  {}
for i in root[2][15]:
    involume[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#link attribute inyear - use link id
print("creating link inyear dictionary")
inyear =  {}
for i in root[2][15]:
    inyear[i.attrib['ITEM-ID']] = i[0].text
print("done!")

#parse the links, build three dictionaries, one of just links, and two of each obj type O1, and O2.
#similar building indicies, will be useful for Publication structure building
links_by_id = {}   # key: ID, value( O1-ID, O2-ID )
links_by_O1 = {}   # key: O1-ID, value: [ (ID, O2-ID), ... ]
links_by_O2 = {}   # key: O2-ID, value: [ (ID, O1-ID), ... ]
print("creating link lookup dictionaries")
for i in root[1]:
    links_by_id[i.attrib['ID']] = (i.attrib['O1-ID'], i.attrib['O2-ID'])
    if i.attrib['O1-ID'] not in links_by_O1:
        links_by_O1[i.attrib['O1-ID']] = [(i.attrib['ID'], i.attrib['O2-ID'])]
    else:
        links_by_O1[i.attrib['O1-ID']].append((i.attrib['ID'], i.attrib['O2-ID']))
    if i.attrib['O2-ID'] not in links_by_O2:
        links_by_O2[i.attrib['O2-ID']] = [(i.attrib['ID'], i.attrib['O1-ID'])]
    else:
        links_by_O2[i.attrib['O2-ID']].append((i.attrib['ID'], i.attrib['O1-ID']))

print("done!")

#no longer need the XML structure
root = 0
tree = 0 

#python structures that will eventually turn into json objects
Person = { "docs": [] }
Publication = { "docs": [] }

# paper, person, phdthesis, proceedings, book, journal, msthesis, www
itr = [ MAX, MAX, MAX, MAX, MAX, MAX, MAX, MAX ]
per_thrs = 0
pub_thrs = 0
per_cnt = 1
pub_cnt = 1

print("Producing JSON structures")

#object-types JSON builder
for k, v in objects.items():

    tmp = {}
    # person goes into the Person data structure
    if v == "person":
        if itr[1] > 0 or MAX == 0:
            per_thrs=per_thrs+1
            itr[1] = itr[1] - 1
            tmp["_id"] = k 
            tmp["Name"] = names[ k ]
            tmp["Type"] = v
            if k in links_by_O1:
                tmp["Works Written"] = []
                for lid, lO2 in links_by_O1[k]:
                    if lO2 in objects:
                        if (objects[lO2] == "paper" or 
                            objects[lO2] == "www" or 
                            objects[lO2] == "book" or 
                            objects[lO2] == "proceedings" or 
                            objects[lO2] == "msthesis" or 
                            objects[lO2] == "phdthesis") and linktype[lid] == "author-of":
                            tmp["Works Written"].append(lO2)

            if k in links_by_O1:
                tmp["Works Edited"] = []
                for lid, lO2 in links_by_O1[k]:
                    if lO2 in objects:
                        if (objects[lO2] == "proceedings" or 
                            objects[lO2] == "www" or
                            objects[lO2] == "book" or
                            objects[lO2] == "paper") and linktype[lid] == "editor-of":
                            tmp["Works Edited"].append(lO2)
     
            Person['docs'].append(tmp)

    #write out files based on our threshold if threshold is set to zero, bypass this
    if(THRESHOLD != 0 and per_thrs == THRESHOLD):
        print("Writing Person JSON file " + str(per_cnt))
        per_thrs = 0
        with open('dblp_Person_thrs' + str(THRESHOLD) + '_file' + str(per_cnt) + '.json', 'w') as outfile:
            json.dump(Person, outfile, indent=4)
        
        per_cnt = per_cnt + 1
        Person = { "docs": [] }

    #everything else goes to the Publication DB
    if v == "paper":
        if itr[0] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[0] = itr[0] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            #reserve link look up for paper authors
            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)

            #editors of this paper
            if k in links_by_O2:
                tmp["Editors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "editor-of":
                           tmp["Editors"].append(lO1)



            #citations lookup forward, could cite books or other papers
            if k in links_by_O1:
                tmp["Citations"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if (objects[lO2] == "paper" or 
                           objects[lO2] == "www" or 
                           objects[lO2] == "proceedings" or 
                           objects[lO2] == "phdthesis" or 
                           objects[lO2] == "msthesis" or 
                           objects[lO2] == "www" or 
                           objects[lO2] == "book") and linktype[lid] == "cites":
                           tmp["Citations"].append(lO2)

            #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if (objects[lO1] == "paper" or
                           objects[lO1] == "proceedings" or 
                           objects[lO1] == "book") and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)

            #add the paper title
            tmp["Title"] = title[k]

            #in-collection, paper->book mapping.
            if k in links_by_O1:
                tmp["in_collection"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if objects[lO2] == "book" and linktype[lid] == "in-collection":
                           tmp["in_collection"].append(lO2)

            #in-proceedings, only links are paper->proceeding, so grab proceeding based on paper id
            if k in links_by_O1:
                tmp["in_proceedings"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if objects[lO2] == "proceedings" and linktype[lid] == "in-proceedings":
                           tmp["in_proceedings"].append(lO2)

            #in-journal, similar to proceedings
            if k in links_by_O1:
                tmp["in_journal"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if objects[lO2] == "journal" and linktype[lid] == "in-journal":
                           tmp["in_journal"].append(lO2)

            Publication['docs'].append(tmp)

 
    if v == "phdthesis":
        if itr[2] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[2] = itr[2] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            tmp["Year"] = year[k]
            tmp["Title"] = title[k]
            if k in publisher:
                tmp["Publisher"] = publisher[k]
            if k in isbn:
                tmp["Isbn"] = isbn[k]
            if k in series:
                tmp["Series"] = series[k]

            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)

           #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if (objects[lO1] == "paper" or
                           objects[lO1] == "proceedings" or 
                           objects[lO1] == "book") and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)


            
            Publication['docs'].append(tmp)
 
    if v == "proceedings":
        if itr[3] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[3] = itr[3] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            if k in volume:
                tmp["Volume"] = volume[k]
            tmp["Year"] = year[k]
            tmp["Title"] = title[k]
            if k in conference:
                tmp["Conference"] = conference[k]
            if k in publisher:
                tmp["Publisher"] = publisher[k]

            #reserve link look up authors
            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)

            #reverse link look up for editors 
            if k in links_by_O2:
                tmp["Editors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "editor-of":
                           tmp["Editors"].append(lO1)

            #citations lookup forward, could cite books or other papers
            if k in links_by_O1:
                tmp["Citations"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if (objects[lO2] == "paper" or 
                           objects[lO2] == "proceedings" or 
                           objects[lO2] == "book") and linktype[lid] == "cites":
                           tmp["Citations"].append(lO2)

            #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if (objects[lO1] == "paper" or
                           objects[lO1] == "proceedings" or 
                           objects[lO1] == "book") and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)

 

            #reverse look-up, what papers are in this journal, could be none!
            if k in links_by_O2:
                tmp["Papers Contained"] = []
                tmp["pages"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       #paper id serves as the key, proceedings only has "pages" attribute
                       if (objects[lO1] == "paper" and linktype[lid] == "in-proceedings"):
                           tmp["Papers Contained"].append(lO1)
                           if lid in pages:
                            #kinda ugly but not worth the effort to fix
                            tmp["pages"].append(pages[lid])
 
            Publication['docs'].append(tmp)

    if v == "book":
        if itr[4] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[4] = itr[4] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            if k in volume:
                tmp["Volume"] = volume[k]
            tmp["Year"] = year[k]
            tmp["Title"] = title[k]
            if k in publisher:
                tmp["Publisher"] = publisher[k]
            if k in isbn:
                tmp["Isbn"] = isbn[k]
            if k in series:
                tmp["Series"] = series[k]

            #Authors
            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)

            #Editors
            if k in links_by_O2:
                tmp["Editors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "editor-of":
                           tmp["Editors"].append(lO1)

            #citations lookup forward, could cite books or other papers
            if k in links_by_O1:
                tmp["Citations"] = []
                for lid, lO2 in links_by_O1[k]:
                   if lO2 in objects:
                       if (objects[lO2] == "paper" or 
                           objects[lO2] == "proceedings" or 
                           objects[lO2] == "phdthesis" or 
                           objects[lO2] == "book") and linktype[lid] == "cites":
                           tmp["Citations"].append(lO2)

            #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if (objects[lO1] == "paper" or
                           objects[lO1] == "proceedings" or 
                           objects[lO1] == "book") and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)


            #reverse look-up, what papers are in this book, could be none!
            tmp["Papers Contained"] =  []
            tmp["pages"] = []
            if k in links_by_O2:
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       #paper id serves as the key, proceedings only has "pages" attribute
                       if (objects[lO1] == "paper" and linktype[lid] == "in-collection"): 
                           tmp["Papers Contained"].append(lO1)
                           if lid in pages:
                            #kinda ugly but not worth the effort to fix
                            tmp["pages"].append(pages[lid])
 
            
            Publication['docs'].append(tmp)

    if v == "journal":
        if itr[5] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[5] = itr[5] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            tmp["Title"] = title[k]
            if k in publisher:
                tmp["Publisher"] = publisher[k]
 
            #reverse look-up, what papers are in this journal, could be none!
            tmp["Papers Contained"] = {} 
            if k in links_by_O2:
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       #journal has the most link attributes
                       #paper id serves as the key, for these attributes.
                       if (objects[lO1] == "paper" and linktype[lid] == "in-journal"):
                           tmp["Papers Contained"][lO1] = {} 
                           if lid in month:
                            tmp["Papers Contained"][lO1]["month"] = month[lid]
                           if lid in involume:
                            tmp["Papers Contained"][lO1]["involume"] = involume[lid]
                           if lid in inyear:
                            tmp["Papers Contained"][lO1]["inyear"] = inyear[lid]
                           if lid in pages:
                            tmp["Papers Contained"][lO1]["pages"] = pages[lid]
                           if lid in number:
                            tmp["Papers Contained"][lO1]["number"] = number[lid]
 
            Publication['docs'].append(tmp)

    if v == "msthesis":
        if itr[6] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[6] = itr[6] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            tmp["Year"] = year[k]
            tmp["Title"] = title[k]
            tmp["School"] = school[k]

            #Authors
            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)

            #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "paper" and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)



            Publication['docs'].append(tmp)

    if v == "www":
        if itr[7] > 0 or MAX == 0:
            pub_thrs=pub_thrs+1
            itr[7] = itr[7] - 1
            tmp["_id"] = k 
            tmp["Type"] = v
            if k in year:
                tmp["Year"] = year[k]
            tmp["Title"] = title[k]
            if k in url:
                tmp["Url"] = url[k] 

            #Authors
            if k in links_by_O2:
                tmp["Authors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "author-of":
                           tmp["Authors"].append(lO1)
            #Editors
            if k in links_by_O2:
                tmp["Editors"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "person" and linktype[lid] == "editor-of":
                           tmp["Editors"].append(lO1)

            #reverse look-up, see who cites this paper, could be none!
            if k in links_by_O2:
                tmp["Works Cited By"] = []
                for lid, lO1 in links_by_O2[k]:
                   if lO1 in objects:
                       if objects[lO1] == "paper" and linktype[lid] == "cites":
                           tmp["Works Cited By"].append(lO1)



            Publication['docs'].append(tmp)
 
    #write out files based on our threshold if threshold is set to zero, bypass this
    if(THRESHOLD != 0 and pub_thrs == THRESHOLD):
        print("Writing Publication JSON file " + str(pub_cnt))
        pub_thrs = 0
        with open('dblp_Publication_thrs' + str(THRESHOLD) + '_file' + str(pub_cnt) + '.json', 'w') as outfile2:
            json.dump(Publication, outfile2, indent=4)
        
        pub_cnt = pub_cnt + 1
        Publication = { "docs": [] }


#write out any remaining doics (or the whole file if THRESHOLD == 0), could be empty
with open('dblp_Person_thrs' + str(THRESHOLD) + '_file' + str(per_cnt) + '.json', 'w') as outfile:
        json.dump(Person, outfile, indent=4)
        
#write out any remaining doics (or the whole file if THRESHOLD == 0), could be empty
with open('dblp_Publication_thrs' + str(THRESHOLD) + '_file' + str(pub_cnt) + '.json', 'w') as outfile2:
            json.dump(Publication, outfile2, indent=4)

print("all done!") 
