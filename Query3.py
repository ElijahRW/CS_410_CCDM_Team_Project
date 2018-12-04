"""
Query 3 - Find Co-author distance between two authors.
Team: Green Bay
CS410/510 - Cloud and Cluster Management
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
            Level 0 = Authors are direct co-authors
            Level 1 = Co-author X is a co-author of Y, Y is a co-author of Z, x and Z are level 1 co-authors
            Level 2 = x is level 2 co-authors with co-authors of z
    """

    co_authors = get_coauthors(author_x)
    return get_coauthor_distance_rec(author_y, co_authors, set([]), 0)


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
    Get the coauthors of author, by name
    :param author: (string) Name of author you wish to find co-authors for
    :return: (list(string)) List of all co-authors, by name
    """
    query = {
        "selector": {
            "_id": {
                "$eq": author
            }
        },
        "fields": ["Works Written"]
    }
    # Get list of works that the author has worked on, by id
    works_written = person.find(query)[0]['Works Written']

    coauthor_ids = set([])

    for work in works_written:
        query = {
            "selector": {
                "_id": {
                    "$eq": work
                }
            },
            "fields": ["Authors"]
        }
        # Get ids of authors that worked on this work
        ids = pub.find(query)[0]['Authors']
        # append unique author ids to the set
        coauthor_ids.update(set(ids))
        coauthor_names = []
        for author_id in coauthor_ids:
            query = {
                "selector": {
                    "_id": {
                        "$eq": author_id
                    }
                },
                "fields": ["Name"]
            }
    # Get ids of authors that worked on this work
            coauthor_names.append(person.find(query)[0]['Name'])
    if author in coauthor_names:
        coauthor_names.remove(author)
    return coauthor_ids


# TODO: Create index on name and pass author names here
print(get_coauthor_distance("728536", "728537"))