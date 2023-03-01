from unittest import result
from numpy import outer
from GraDBClasses import *
from RelDBClasses import *
from DMClasses import *
from optparse import Values
from pandas import read_csv
from sqlite3 import connect
from pandas import read_sql, DataFrame, merge, concat
from pandas import concat

class GenericQueryProcessor(object):
    def __init__(self):
        self.queryProcessor = []
        
    def cleanQueryProcessors(self):
        if len(self.queryProcessor) > 0:
            self.queryProcessor = []
            return True
        else:
            return True
            
    def addQueryProcessor(self, QueryProcessor):
        self.QueryProcessor=QueryProcessor
        self.queryProcessor.append(self.QueryProcessor)
        return True


    #ADDITIONAL METHODS 

    def toVenue(self, doi):
        if len(self.queryProcessor) == 0:
            return None
        else:
            if self.queryProcessor[0].__class__.__name__ == "TriplestoreQueryProcessor":
                part_query = self.getVenue(doi, self.queryProcessor[0].getEndpointUrl())
            elif self.queryProcessor[0].__class__.__name__ == "RelationalQueryProcessor":
                part_query = self.getVenue(doi, self.queryProcessor[0].getDbPath())
            a_dict = {}
            for idx in range(len(self.queryProcessor)-1):
                if self.queryProcessor[idx+1].__class__.__name__ == "TriplestoreQueryProcessor":
                    part_query=concat([part_query, self.getVenue(doi, self.queryProcessor[idx+1].getEndpointUrl())],ignore_index = True, axis = 0)
                elif self.queryProcessor[idx+1].__class__.__name__ == "RelationalQueryProcessor":
                    part_query=concat([part_query, self.getVenue(doi, self.queryProcessor[idx+1].getDbPath())],ignore_index = True, axis = 0)
            
            for idx, row in part_query.iterrows():
                key = row["title"]
                if row["title"] not in a_dict:
                    a_dict[row["title"]] = {
                        "id": [row["id"]],
                        "title" :row["title"],
                        "publisher" : self.toPublisher(row["publisher"]),
                    }
                else:
                    if row["id"] not in a_dict[row["title"]]["id"]:
                        a_dict[row["title"]]["id"].append(row["id"])
                    if type(a_dict[row["title"]]["publisher"] == type(None)):
                        a_dict[row["title"]]["publisher"] = self.toPublisher(row["publisher"])
            if len(a_dict) == 0:
                return None
            else:
                if len(self.queryProcessor) == 0:
                    return None
                else:
                    return((Venue(a_dict[key]["id"], a_dict[key]["title"], a_dict[key]["publisher"])))
    
    def toCitedPublication(self, doi, **kwargs):
        if kwargs.get("doi_recalled", None) == doi:
            return []
        else:
            list_of_publication = []
            if self.queryProcessor[0].__class__.__name__ == "TriplestoreQueryProcessor":
                part_query = self.getPublications(doi, self.queryProcessor[0].getEndpointUrl())
            elif self.queryProcessor[0].__class__.__name__ == "RelationalQueryProcessor":
                part_query = self.getPublications(doi, self.queryProcessor[0].getDbPath())
            a_dict = {}
            for idx in range(len(self.queryProcessor)-1):
                if self.queryProcessor[idx+1].__class__.__name__ == "TriplestoreQueryProcessor":
                    part_query=concat([part_query, self.getPublications(doi, self.queryProcessor[idx+1].getEndpointUrl())],ignore_index = True, axis = 0)
                elif self.queryProcessor[idx+1].__class__.__name__ == "RelationalQueryProcessor":
                    part_query=concat([part_query, self.getPublications(doi, self.queryProcessor[idx+1].getDbPath())],ignore_index = True, axis = 0)
            for idx, row in part_query.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : self.toVenue(row["id"]),
                        "authors" : self.getPublicationAuthors(row["id"]),
                        "cites" : self.toCitedPublication(row["id"], doi_recalled=doi),
                    }
            if len(a_dict) > 0:
                for el in a_dict:
                    publication = (Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"]))
                    list_of_publication.append(publication)
                return list_of_publication
            else:
                return []

    def toPublisher(self, id):
        if self.queryProcessor[0].__class__.__name__ == "TriplestoreQueryProcessor":
            part_query = self.getOrganization(id, self.queryProcessor[0].getEndpointUrl())
        elif self.queryProcessor[0].__class__.__name__ == "RelationalQueryProcessor":
            part_query = self.getOrganization(id, self.queryProcessor[0].getDbPath())
        for idx in range(len(self.queryProcessor)-1):
            if self.queryProcessor[idx+1].__class__.__name__ == "TriplestoreQueryProcessor":
                part_query=concat([part_query, self.getOrganization(id, self.queryProcessor[idx+1].getEndpointUrl())],ignore_index = True, axis = 0)
            elif self.queryProcessor[idx+1].__class__.__name__ == "RelationalQueryProcessor":
                part_query=concat([part_query, self.getOrganization(id, self.queryProcessor[idx+1].getDbPath())],ignore_index = True, axis = 0)
        a_dict={}
        for idx, row in part_query.iterrows():
            if row["orgId"] not in a_dict:
                a_dict[row["orgId"]] = {
                    "id" : row["orgId"],
                    "name" : row["orgName"]
                }
            else:
                if type(row["orgName"]) == type("") and row["orgName"] != (a_dict[row["orgId"]])["name"]:
                    a_dict[row["orgId"]]["name"] = row["orgName"]
        if len(a_dict) > 0:
            for el in a_dict:
                return(Organization(a_dict[el]["id"], a_dict[el]["name"]))
        else:
            return None

    def getVenue(self, doi, url_or_path):
        if "http" in url_or_path:
            new_query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?publisher
            WHERE {{
                ?publication schema:identifier ?doi .
                ?publication schema:isPartOf ?venue .
                FILTER (?doi = "{0}")
                OPTIONAL {{?venue schema:identifier ?id .}}
                OPTIONAL {{?venue schema:name ?title }}
                OPTIONAL {{?venue schema:publisher ?publi.
                ?publi schema:identifier ?publisher}}    
            }}
            """
            venues = get(url_or_path, new_query.format(doi), True)
            return venues
        elif url_or_path.endswith(".db"):
            with connect(url_or_path) as con:
                query="""SELECT DISTINCT "issn", "publication_venue", "publisher"  
                FROM Publications 
                LEFT JOIN "Venues_json"
                ON "Publications".id="Venues_json".'venues doi' 
                WHERE "Publications".id = ? """
                dframe = read_sql(query,con, params=[doi])
                dframe = dframe.rename(columns={"issn":"id"})
                dframe = dframe.rename(columns={"publication_venue":"title"})
            return dframe
        
    def getPublications(self, doi, url_or_path):
        if "http" in url_or_path:
            new_query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?id ?title ?publicationYear ?publicationVenue ?author ?cites 
            WHERE {{
                ?publication schema:identifier ?doi .
                ?publication schema:citation ?cites .
                ?cites schema:identifier ?id .
                FILTER (?doi = "{0}")
                OPTIONAL {{?publication schema:name ?title }}
                OPTIONAL {{?publication schema:isPartOf ?publicationVenue .}}
                OPTIONAL {{?publication schema:author ?author }} .
                OPTIONAL {{?publication schema:citation ?cites}}
            }}
            """
            venues = get(url_or_path, new_query.format(doi), True)
            return venues
        elif url_or_path.endswith(".db"):
            with connect(url_or_path) as con:
                query="""SELECT DISTINCT "id", "title", "publication_year", "venues_intid", "author", "cites"
                FROM "References" 
                LEFT JOIN "Publications"
                ON "References".'citation' = "Publications".'id'
                LEFT JOIN Authors_Obj
                ON Publications.id = Authors_Obj.auth_doi
                LEFT JOIN References_Obj
                ON Publications.id = References_Obj.ref_doi
                LEFT JOIN "Venues_json"
                ON "Publications".id="Venues_json".'venues doi'
                WHERE ref_id = ? """
                dframe =read_sql(query,con,params=[doi])
                dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue"})
            return dframe

    def getOrganization(self, id, url_or_path):
        if "http" in url_or_path: 
            new_query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>

            SELECT ?orgId ?orgName
            WHERE {{
                ?publisher schema:identifier ?orgId.
                FILTER (?orgId = "{0}") .
                OPTIONAL {{?publisher schema:name ?orgName }}  
            }}
            """
            publ = get(url_or_path, new_query.format(id), True)
            return publ
        elif url_or_path.endswith(".db"):
            with connect (url_or_path) as con:
                query="""SELECT DISTINCT "publisher id", "name"
                FROM Publishers
                LEFT JOIN "Venues csv"
                ON "Publishers"."publisher id"="Venues csv".'publisher'
                LEFT JOIN "Venues_json"
                ON "Venues_json"."venues doi"="Venues csv".'publication doi'
                WHERE "Venues csv".'publisher' = ? """
                dframe = read_sql(query,con, params=[id])
                dframe = dframe.rename(columns={"publisher id":"orgId", "name":"orgName"})
            return dframe

    def getPublicationsPublishedInYear(self, year):
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getPublicationsPublishedInYear(year)
            a_dict = {}
            listOfPublication = []
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsPublishedInYear(year)],ignore_index = True, axis = 0)
            for idx, row in part_query.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : self.toVenue(row["id"]),
                        "authors" : self.getPublicationAuthors(row["id"]),
                        "cites" : self.toCitedPublication(row["id"]),
                    }
            if len(a_dict) > 0:
                for el in a_dict:
                    listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
            return listOfPublication
    
    def getPublicationsByAuthorId(self, id):
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getPublicationsByAuthorId(id)
            a_dict = {}
            listOfPublication = []
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsByAuthorId(id)],ignore_index = True, axis = 0)
            for idx, row in part_query.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" : self.toVenue(row["id"]),
                        "authors" : self.getPublicationAuthors(row["id"]),
                        "cites" : self.toCitedPublication(row["id"]),
                    }
            if len(a_dict) > 0:
                for el in a_dict:
                    listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
            return listOfPublication
        
    def getMostCitedPublication(self):
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getMostCitedPublication()
            for idx in range(len(self.queryProcessor)-1):
                self.queryProcessor[idx+1].getMostCitedPublication()
                part_query=concat([part_query, self.queryProcessor[idx+1].getMostCitedPublication()],ignore_index = True, axis = 0)
            sorted_query=part_query.sort_values(by='value_occurrence', ascending=False, ignore_index=True)
            if len(sorted_query)>1:
                col_ind=6
                result_df=DataFrame()
                for idx, row in sorted_query.iterrows():
                    if idx==0:
                        result_df=concat([sorted_query.loc[[0]]])
                    if idx>0:
                        if sorted_query.iloc[idx, col_ind]==sorted_query.iloc[idx-1, col_ind]:
                            result_df=concat([result_df, sorted_query.loc[[idx]]])
                        if sorted_query.iloc[idx, col_ind]<sorted_query.iloc[idx-1, col_ind]:
                            result_df
            else:
                result_df=sorted_query
            a_dict = {}
            listOfPublication = []
            for idx, row in result_df.iterrows():
                if row["id"] not in a_dict:
                    a_dict[row["id"]] = {
                        "id": row["id"],
                        "title" :row["title"],
                        "publicationYear" : row["publicationYear"],
                        "publicationVenue" :  self.toVenue(row["id"]),
                        "authors" : self.getPublicationAuthors(row["id"]),
                        "cites" : self.toCitedPublication(row["id"]),
                    }       
            if len(a_dict) > 0:
                for el in a_dict:
                    listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
            if len(listOfPublication) > 0:
                return listOfPublication[0]
            else:
                return None
        
    def getMostCitedVenue(self): #CONFRONTO CON COUNT DEL TRIPLESTORE MA COME? SI PUO' VISUALIZZARE IL COUNT DEL TRIPLESTORE? QUELLO DEL RELATIONAL SI'!
        if len(self.queryProcessor) == 0:
            return None
        else:
            part_query = self.queryProcessor[0].getMostCitedVenue()
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getMostCitedVenue()],ignore_index = True, axis = 0)
            sorted_query=part_query.sort_values(by='value_occurrence', ascending=False, ignore_index=True)
            if len(sorted_query)>1:
                col_ind=3
                result_df=DataFrame()
                for idx, row in sorted_query.iterrows():
                    if idx==0:
                        result_df=concat([sorted_query.loc[[0]]])
                    if idx>0:
                        if sorted_query.iloc[idx, col_ind]==sorted_query.iloc[idx-1, col_ind]:
                            result_df=concat([result_df, sorted_query.loc[[idx]]])
                        if sorted_query.iloc[idx, col_ind]<sorted_query.iloc[idx-1, col_ind]:
                            result_df=result_df
            else:
                result_df=sorted_query
            a_dict = {}
            listOfVenue = []
            for idx, row in result_df.iterrows():
                if row["title"] not in a_dict:
                    a_dict[row["title"]] = {
                        "id": [row["id"]],
                        "title" :row["title"],
                        "publisher" : self.toPublisher(row["publisher"]),
                    }
                else:
                    if row["id"] not in a_dict[row["title"]]["id"]:
                        a_dict[row["title"]]["id"].append(row["id"])
                    if type(a_dict[row["title"]]["publisher"] == type(None)):
                        a_dict[row["title"]]["publisher"] = self.toPublisher(row["publisher"])
            if len(a_dict) > 0:
                for el in a_dict:
                    listOfVenue.append((Venue(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"])))
            if len(listOfVenue) > 0:
                return listOfVenue[0]
            else:
                return None
        
    def getVenuesByPublisherId(self, id):
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getVenuesByPublisherId(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getVenuesByPublisherId(id)],ignore_index = True, axis = 0)
        a_dict = {}
        listOfVenues = []
        for idx, row in part_query.iterrows():
            if row["title"] not in a_dict:
                a_dict[row["title"]] = {
                    "id": [row["id"]],
                    "title" :row["title"],
                    "publisher" : self.toPublisher(id),
                }
            else:
                if row["id"] not in a_dict[row["title"]]["id"]:
                    a_dict[row["title"]]["id"].append(row["id"])
                if type(a_dict[row["title"]]["publisher"]) == type(None):
                    a_dict[row["title"]]["publisher"] = self.toPublisher(id)
        if len(a_dict) > 0:
            for el in a_dict:
                listOfVenues.append((Venue(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"])))
        return listOfVenues
        
    def getPublicationInVenue(self, id):
        a_dict = {}
        listOfPublication = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getPublicationInVenue(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationInVenue(id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" :  self.toVenue(row["id"]),
                    "authors" : self.getPublicationAuthors(row["id"]),
                    "cites" : [self.toCitedPublication(row["id"])],
                }
        if len(a_dict) > 0:
            for el in a_dict:
                listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
        return listOfPublication
    
    def getJournalArticlesInIssue(self, issue, volume, id):
        a_dict = {}
        listOfPublication = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInIssue(issue, volume, id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInIssue(issue, volume, id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" :  self.toVenue(row["id"]),
                    "authors" : self.getPublicationAuthors(row["id"]),
                    "cites" : [self.toCitedPublication(row["id"])],
                    "issue": row["issue"],
                    "volume": row["volume"]
                }
        if len(a_dict) > 0:
            for el in a_dict:
                listOfPublication.append((JournalArticle(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["issue"], a_dict[el]["volume"])))
        return listOfPublication
        
    def getJournalArticlesInVolume(self, volume, id):
        a_dict = {}
        listOfPublication = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInVolume(volume, id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInVolume(volume, id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" :  self.toVenue(row["id"]),
                    "authors" : self.getPublicationAuthors(row["id"]),
                    "cites" : [self.toCitedPublication(row["id"])],
                    "issue": row["issue"],
                    "volume": row["volume"]
                }
        if len(a_dict) > 0:        
            for el in a_dict:
                listOfPublication.append((JournalArticle(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["issue"], a_dict[el]["volume"])))
        return listOfPublication
        
    def getJournalArticlesInJournal(self, id):
        a_dict = {}
        listOfPublication = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getJournalArticlesInJournal(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getJournalArticlesInJournal(id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" :  self.toVenue(row["id"]),
                    "authors" : self.getPublicationAuthors(row["id"]),
                    "cites" : [self.toCitedPublication(row["id"])],
                    "issue": row["issue"],
                    "volume": row["volume"]
                }
        if len(a_dict) > 0:        
            for el in a_dict:
                listOfPublication.append((JournalArticle(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"], a_dict[el]["issue"], a_dict[el]["volume"])))
        return listOfPublication

    def getProceedingsByEvent(self, event):
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getProceedingsByEvent(event)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getProceedingsByEvent(event)],ignore_index = True, axis = 0)
        a_dict = {}
        listOfProc = []
        for idx, row in part_query.iterrows():
            if row["title"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": [row["id"]],
                    "title" :row["title"],
                    "publisher" : self.toPublisher(row["publisher"]),
                    "event" : row["event"]
                }
            else:
                if row["id"] not in a_dict[row["title"]]["id"]:
                    a_dict[row["title"]]["id"].append(row["id"])
                if type(a_dict[row["title"]["publisher"] == type(None)]):
                    a_dict[row["title"]["publisher"]] = self.toPublisher(row["publisher"])
        if len(a_dict) > 0:        
            for el in a_dict:
                listOfProc.append((Proceedings(a_dict[el]["id"], a_dict[el]["title"], a_dict[el]["publisher"], a_dict[el]["event"])))
        return listOfProc
            
    
    def getPublicationAuthors(self, id):
        a_dict = {}
        listOfAuthors = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getPublicationAuthors(id)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationAuthors(id)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "familyName" :row["familyName"],
                    "givenName" : row["givenName"],
                }
        if len(a_dict) > 0:
            for el in a_dict:
                listOfAuthors.append((Person(a_dict[el]["id"], a_dict[el]["givenName"], a_dict[el]["familyName"])))
        return listOfAuthors

    def getPublicationsByAuthorName(self, name):
        a_dict = {}
        listOfPublication = []
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getPublicationsByAuthorName(name)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getPublicationsByAuthorName(name)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "title" :row["title"],
                    "publicationYear" : row["publicationYear"],
                    "publicationVenue" :  self.toVenue(row["id"]),
                    "authors" : self.getPublicationAuthors(row["id"]),
                    "cites" : [self.toCitedPublication(row["id"])],
                }
        if len(a_dict) > 0:
            for el in a_dict:
                listOfPublication.append((Publication(a_dict[el]["id"], set(a_dict[el]["authors"]),a_dict[el]["publicationYear"], a_dict[el]["title"], a_dict[el]["publicationVenue"], a_dict[el]["cites"])))
        return listOfPublication
        
    def getDistinctPublisherOfPublications(self, doi):
        a_dict = {}
        listOfPublishers = [] 
        if len(self.queryProcessor) == 0:
            return []
        else:
            part_query = self.queryProcessor[0].getDistinctPublisherOfPublications(doi)
            for idx in range(len(self.queryProcessor)-1):
                part_query=concat([part_query, self.queryProcessor[idx+1].getDistinctPublisherOfPublications(doi)],ignore_index = True, axis = 0)
        for idx, row in part_query.iterrows():
            if row["id"] not in a_dict:
                a_dict[row["id"]] = {
                    "id": row["id"],
                    "name" :row["name"],
                }
            else:
                if type(row["name"]) == type("str") and row["name"] not in (a_dict[row["id"]]):
                    (a_dict[row["id"]])["name"] = row["name"]
        if len(a_dict) > 0:
            for el in a_dict:
                listOfPublishers.append((Organization(a_dict[el]["id"], a_dict[el]["name"])))
        return listOfPublishers
    
#gen_query.cleanQueryProcessors()
#gen_query.addQueryProcessors(rlp_query)
#gen_query.addQueryProcessors(grp_qp)
#gen_query.getPublicationsPublishedInYear(2020) #OK
#print(gen_query.getPublicationsByAuthorId("0000-0001-7412-4776"))
#print(gen_query.getMostCitedPublication())
#print(gen_query.getMostCitedVenue())
#print(gen_query.getVenuesByPublisherId("crossref:2373")) #OK
#print(gen_query.getPublicationInVenue("issn:2164-551")) #OK
#print(gen_query.getJournalArticlesInIssue("1", "12", "issn:1758-2946")) #OK
#print(gen_query.getJournalArticlesInVolume("17", "issn:2164-5515")) #OK 
#print(gen_query.getJournalArticlesInJournal("issn:2164-551")) #OK 
#gen_query.getProceedingsByEvent("web") 
#print(gen_query.getPublicationAuthors("doi:10.1080/21645515.2021.1910000")) #OK
#print(gen_query.getPublicationsByAuthorName("David"))
#print(gen_query.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))
