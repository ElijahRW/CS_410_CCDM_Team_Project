"""
Query 3 - Find Co-author distance between two authors.
Team: Green Bay
CS410/510 - Cloud and Cluster Management

Find co author Distance between Michael J. Franklin (id: "747452")
and
Moshe Y. Vardi(id: 729526)
"""

import couchdb

# connect to the server
server = couchdb.client.Server('http://35.233.173.113:5984/')
server.login(name='admin', password='cloudcluster')

# Get the two databases
# person stores author documents, publication stores all written works documents
person = server['person']
pub = server['publication']


def get_coauthor_distance(author_x, author_y):
    """
    Return the co-author distance between x and y
    :param (string) author_x: Name of the first author
    :param (string) author_y: Name of the second author
    :return: (int) distance between x and y
            Level 1 = Authors are direct co-authors
            Level 2 = Co-author X is a co-author of Y, Y is a co-author of Z, x and Z are level 1 co-authors
            Level 3 = x is level 2 co-authors with co-authors of z
    """
    query = {
        "selector": {
            "Name": {
                "$eq": author_x
            }
        }
    }
    x_id = person.find(query)[0]["_id"]
    query = {
        "selector": {
            "Name": {
                "$eq": author_y
            }
        }
    }
    y_id = person.find(query)[0]["_id"]

    co_authors = get_coauthors(x_id)
    return get_coauthor_distance_rec(y_id, co_authors, set([]), 1)


def get_coauthor_distance_rec(author, co_authors, seen, level):
    '''
    Return the level of co-author between two authors.
    If author is in co_authors then return level
    Pass the next level of co-authors(all co-authors of each person in the co_authors list)
    :param author: Author we are searching for
    :param co_authors: list of authors to search in.
    :param seen: List of authors we have already seen
    :param level: Current co-author distance
    :return: int distance between co-authors
    '''
    print("Checking Level", level)
    if author in co_authors:
        return level
    else:
        # add this level to the seen set
        seen.update(co_authors)
        # Get the next level of co_authors
        next_level_authors = set([])
        for auth in co_authors:
            next_level_authors.update(get_coauthors(auth))
        # remove authors we have already seen
        next_level_authors -= seen
        # if no more authors we haven't seen, then halt, authors have no connection
        if next_level_authors == set([]):
            return -1
        return get_coauthor_distance_rec(author, next_level_authors, seen, level+1)


def get_coauthors(author):
    """
    Get the coauthors of author, by id
    :param author: (string) Name of author you wish to find co-authors for
    :return: (list(string)) List of all co-authors, by name
    """
    # Get list of works that the author has worked on, by id
    works_written = person.get(author)['Works Written']

    coauthor_ids = set([])
    for work in works_written:
        # Get ids of authors that worked on this work
        ids = pub.get(work)['Authors']
        # append unique author ids to the set
        coauthor_ids.update(set(ids))

    return coauthor_ids


# Get co-author distance between Moshe Y. Vardi and Michael J. Franklin
print(get_coauthor_distance("Moshe Y. Vardi", "Michael J. Franklin"))
