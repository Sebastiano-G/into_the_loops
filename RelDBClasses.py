import sqlite3
from pandas import read_csv
from sqlite3 import connect
from pandas import read_sql, DataFrame, merge, concat



class RelationalProcessor(object):
    def __init__(self):
        self.dbPath = ''


    def getDbPath(self):
        return self.dbPath

    def setDbPath(self, new_path):
        self.dbPath= new_path
        return True



class RelationalDataProcessor(RelationalProcessor):
    def __init__(self):
        #maybe this is extra
        super().__init__()



    def uploadData(self, file_path): 
        from pandas import read_sql, DataFrame
        from sqlite3 import connect
        
        #First let's populate our database with empty tables that have appropriate columns, to avoid queries failing 
        #completely in case some file is not uploaded.
        
        df_publications_empty=DataFrame (columns=["publicationInternalId", "id", "title", "type", "publication_year", "issue", "volume", "chapter", "publication_venue",
                                                "venue_type", "publisher", "event"], dtype="string")
        
        global_df_json_empty = DataFrame(columns=["venues_intid", "venues doi", "issn", "pub id", "auth doi", 
                                                  "auth_id", "doi", "family_name", "given_name", "orcid"], dtype="string")
        
        auth_and_publications_empty= DataFrame(columns=["pub id", "auth doi", "auth_id", "doi", 
                                                        "family_name", "given_name", "orcid"], dtype="string")
        
        person_df_empty= DataFrame(columns=["auth_id", "doi", "family_name", "given_name", "orcid"], dtype="string")
        
        auth_obj_empty = DataFrame(columns=["auth_doi", "author"], dtype="string")
        
        references_empty = DataFrame(columns=["ref_id", "ref doi", "citation"], dtype="string")
        
        ref_obj_empty= DataFrame(columns=["ref_doi", "cites"],dtype="string")
        
        publishers_empty= DataFrame(columns= ["pub_id", "publisher id", "name"], dtype="string")
        
        venues_json_empty= DataFrame(columns= ["venues_intid", "venues doi", "issn"], dtype="string")
        
        
        with connect(self.dbPath) as con:

                    df_publications_empty.to_sql("Publications", con, if_exists="append", index=False)
                    global_df_json_empty.to_sql("Global_json_dataframe", con, if_exists="append", index=False)
                    auth_and_publications_empty.to_sql("Authors and publications", con, if_exists="append", index=False)
                    person_df_empty.to_sql("Person", con, if_exists="append", index=False)
                    auth_obj_empty.to_sql("Authors_Obj", con, if_exists="append", index=False)
                    references_empty.to_sql("References", con, if_exists="append", index=False)
                    ref_obj_empty.to_sql("References_Obj", con, if_exists="append", index=False)
                    publishers_empty.to_sql("Publishers", con, if_exists="append", index=False)
                    venues_json_empty.to_sql("Venues_json", con, if_exists="append", index=False)
                    con.commit()
                    
                    
        #Now, let's upload our data accordingly            
        
        if ".csv" in file_path:
            from csv import reader
            with open (file_path, "r", encoding= "utf-8") as d:
                from pandas import Series, DataFrame, read_csv
                df_publications = read_csv (file_path, keep_default_na=False,
                                        dtype={
                                            "id": "string",
                                            "title": "string",
                                            "type":"string",
                                            "publication_year":"int",
                                            "issue": "string",
                                            "volume": "string",
                                            "chapter":"string",
                                            "publication_venue": "string",
                                            "venue_type": "string",
                                            "publisher":"string",
                                            "event":"string"
                                        })
                df_publications

                #Insert a series of internal ids for each publication
                
                publication_dois= df_publications[["id"]]
                publications_internal_id = []
                for idx, row in publication_dois.iterrows():

                    publications_internal_id.append("publication-" + str(idx))
                df_publications.insert(0, "publicationInternalId", Series(publications_internal_id, dtype="string"))
                df_publications


                #publishers
                publishers_l= (df_publications['publisher'].tolist())
                publishers_l


                publishers_unique = []
                [publishers_unique.append(x) for x in publishers_l if x not in publishers_unique]

                publishers_unique
                pub_int_id=[]
                for item in publishers_unique:
                    pub_int_id.append("pub-" + str(publishers_unique.index(item)))
                pub_int_id

                from pandas import concat, merge
                unique_publishers= Series(publishers_unique, name ="Publishers")
                publisher_int_id = Series(pub_int_id, name= "publisher_internal_id")
                unique_publishers_with_id = concat([unique_publishers, publisher_int_id],axis=1)


                #Retrieve data for journal articles

                journal_articles = df_publications.query("type == 'journal-article'")

                #Retrieve data for book chapters
                book_chapters = df_publications.query("type == 'book-chapter'")
                #Retrieve data for proceedings papers
                proceedings_paper = df_publications.query("type == 'proceedings-paper'")
            

                #Retrieve data for journals,books and proceedings
                journals = df_publications.query("venue_type == 'journal'")
                books = df_publications.query("venue_type == 'book'")
                proceedings = df_publications.query("venue_type == 'proceedings'")

                venues_csv=df_publications[["id","publication_venue", "venue_type","publisher"]]
                venues_csv.columns = ['publication doi', 'title', 'type', 'publisher']
                venues_csv=venues_csv.drop_duplicates()


                

                from sqlite3 import connect
                with connect(self.dbPath) as con:


                    df_publications.to_sql("Publications", con, if_exists="append", index=False)
                    venues_csv.to_sql("Venues csv", con, if_exists="append", index=False) 
                    books.to_sql("Books", con, if_exists="append", index=False )
                    journals.to_sql("Journals",con, if_exists="append", index=False)
                    book_chapters.to_sql("BookChapters", con, if_exists="append", index=False)
                    journal_articles.to_sql("JournalArticles", con, if_exists="append", index=False)
                    proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="append", index=False)
                    proceedings.to_sql("Proceedings", con, if_exists="append", index=False)
                    unique_publishers_with_id.to_sql("Publishers csv", con, if_exists="append", index=False)

                    con.commit()
                    #Dataframes have been added with values from the csv

            return True

        elif ".json" in file_path:
            from pandas import Series, DataFrame
            from json import load
            with open (file_path, "r", encoding= "utf-8") as f:
                other_data= load(f)

                #separate each key from the dictionary

                other_data_authors = other_data.get("authors")
                other_data_venues = other_data.get("venues_id")
                other_data_ref = other_data.get("references")
                other_data_pub = other_data.get("publishers")

                #initialize empty lists for author data to be stored
                auth_doi=[]
                auth_fam=[]
                auth_giv=[]
                auth_id=[]

                for key in other_data_authors:   #each key has three values/dictionaries, name, given name and orcid.
                    a=[]
                    b=[]
                    c=[]
                    auth_val = other_data_authors[key] #for each key of the dictionary key which is the doi, append it to appropriate list
                    auth_doi.append(key)
                    for dict in auth_val:
                        a.append(dict["family"])  #for each dictionary of the doi-key, append its values to appropriate lists
                        b.append(dict["given"])
                        c.append(dict["orcid"])
                    auth_fam.append(a)            #populate the lists with the values
                    auth_giv.append(b)
                    auth_id.append(c)
                auth_doi_s=Series(auth_doi)       #transform lists to Series
                auth_fam_s=Series(auth_fam)

                auth_giv_s=Series(auth_giv)

                auth_id_s=Series(auth_id)

                                                  #create dataframe from the Series
                authors_df=DataFrame({
                    "auth doi" : auth_doi_s,
                    "family name" : auth_fam_s,
                    "given name" : auth_giv_s,
                    "orcid" : auth_id_s
                })
                authors_df
            


                dois_auth=authors_df[["auth doi"]]   #create and append internal id for the authors
                doi_int_id=[]
                for idx,row in dois_auth.iterrows():
                    doi_int_id.append("pub-" + str(idx))

                authors_df.insert(0, "pub id", Series(doi_int_id, dtype="string"))

                pub_id_and_doi= authors_df[["pub id", "auth doi"]]
                pub_id_and_doi.drop_duplicates

                #Create a dataframe for unique persons, skipping duplicates by not iterating over values already stored.
                #This method removes lists

                person_fam=[]
                person_giv=[]
                person_orcid=[]
                corr_doi=[]

                person_internal_id=[]
                a=0
                for names_list in auth_fam:
                    i=auth_fam.index(names_list, a)
                    a +=1
                    for name in names_list:
                        person_fam.append(name)
                        corr_doi.append(auth_doi[i])
                for given_list in auth_giv:
                    for name in given_list:
                        person_giv.append(name)
                for id_list in auth_id:
                    for id in id_list:
                        person_orcid.append(id)
                idx=0
                prov_list=[]
                for orcid in person_orcid:
                    if orcid not in prov_list:
                        prov_list.append(orcid)
                        person_internal_id.append("person-" + str(idx))
                        idx+=1
                    else:
                        idxj=prov_list.index(orcid)
                        person_internal_id.append(person_internal_id[idxj])
                #Dataframe for unique persons

                person_df=DataFrame({
                    "auth_id":person_internal_id,
                    "doi":corr_doi,
                    "family_name" : person_fam,
                    "given_name" : person_giv,
                    "orcid" : person_orcid
                })
                unique_doi=[]
                
                list_authors=[]
                prov_list=[]
                string_list=[]
                idx=0
                for doi in corr_doi:
                    if doi not in unique_doi:
                        unique_doi.append(doi) 
                        prov_list=[]
                        prov_list.append(person_internal_id[idx])
                        list_authors.append(prov_list)
                        idx+=1
                    else:
                        prov_list.append(person_internal_id[idx])
                        idx+=1
                for list in list_authors:
                    string_authors=", ".join(list)
                    string_list.append(string_authors)

                authors_list=DataFrame({ "auth_doi":unique_doi, "author":string_list})       
                #create a dataframe with each person and their publications
                from pandas import merge
                auth_pub_with_id=merge(pub_id_and_doi, person_df, left_on="auth doi", right_on="doi")
                auth_pub_with_id



                # """Venues dataframe.
                # Same strategies as before but values of the dois' keys are not other dictionaries but lists
                # """

                venues_doi=[]
                venues_issn=[]

                for key in other_data_venues:
                    venues_val = other_data_venues[key]
                    venues_doi.append(key)
                    venues_issn.append(venues_val)

                venues_doi_s=Series(venues_doi)
                venues_issn_s=Series(venues_issn)


                #Create dataframe with the values of the json file.
                venues_df=DataFrame({
                    "venues doi" : venues_doi_s,
                    "issn" : venues_issn_s,
                })
                venues_df

                from pandas import merge

                venues_and_authors = merge(person_df, venues_df, left_on="doi", right_on="venues doi")
                venues_and_authors

                venues_doi=[]
                venues_issn=[]

                for key in other_data_venues:  #for each key- doi in our venues dictionary
                    venues_val = other_data_venues[key]
                    venues_doi.append(key)     #append the key, aka the doi.
                    venues_issn.append(venues_val) #append its value, aka the issn number.

                unique_venues=[]
                list_dois=[]


                #Create lists for the dataframe which will contain unique venues
                for i in range(len(venues_issn)):
                    if venues_issn[i] not in unique_venues:
                        unique_venues.append(venues_issn[i])
                        dois = []
                        dois.append(venues_doi[i])
                        list_dois.append(dois)
                    else:
                        j = unique_venues.index(venues_issn[i])
                        list_dois[j].append(venues_doi[i])

                un_venues_doi_s=Series(list_dois)
                un_venues_issn_s=Series(unique_venues)
                unique_venues_df=DataFrame({
                    "venues doi" : un_venues_doi_s,
                    "issn" : un_venues_issn_s,
                })

                unique_venues_df


                #Create internal identifiers for venues

                venues_ids = unique_venues_df[["venues doi", "issn"]]
                venues_internal_id = []
                for idx, row in venues_ids.iterrows():
                    venues_internal_id.append("venues-" + str(idx))
                venues_ids.insert(0, "venues id", Series(venues_internal_id, dtype="string"))
                venues_ids



                #Remove duplicates
                venues_id_intid=venues_ids.filter(["venues id"])
                venues_id_intid=venues_id_intid.values.tolist()
                venues_id_doi=venues_ids.filter(["venues doi"])
                venues_id_doi=venues_id_doi.values.tolist()
                venues_id_issn=venues_ids.filter(["issn"])
                venues_id_issn=venues_id_issn.values.tolist()

                l_intid=[]
                l_doi=[]
                l_issn=[]
                for outlist in venues_id_doi:
                    i=venues_id_doi.index(outlist)
                    for item in outlist:
                        for doi in item:
                            l_doi.append(doi)
                            l_intid.append(venues_id_intid[i][0])
                            str_issn=", ".join(venues_id_issn[i][0])
                            l_issn.append(str_issn)
                s_intid=Series(l_intid)
                s_doi=Series(l_doi)
                s_issn=Series(l_issn)
                unique_venues_ids_df=DataFrame({"venues_intid":s_intid, "venues doi":s_doi, "issn":s_issn})
                unique_venues_ids_df.drop_duplicates(subset="issn")
                


                


                #References dataframe
                # Same as venues, values of the keys are lists
                # """

                ref_doi=[]
                ref_cit=[]

                for key in other_data_ref:
                    ref_list = other_data_ref[key]
                    if len(ref_list)>0:
                        for cit in ref_list:
                            ref_doi.append(key)
                            ref_cit.append(cit)
                    else:
                        ref_doi.append(key)
                        ref_cit.append(ref_list)

                ref_doi_s=Series(ref_doi)
                ref_cit_s=Series(ref_cit)

                
                #Create the dataframe for references
                new_ref_df=DataFrame({
                    "ref doi" : ref_doi_s,
                    "citation" : ref_cit_s, 
                })
                
                ref_df=new_ref_df.astype({"citation": str}, errors='raise')
                
                ref_ids = ref_df[["ref doi", "citation"]]
                ref_internal_id = []
                for idx, row in ref_ids.iterrows():
                    ref_internal_id.append("citation-" + str(idx))
                ref_ids.insert(0, "ref_id", Series(ref_internal_id, dtype="string"))
                
                unique_doi=[]
                list_ref=[]
                prov_list=[]
                string_list=[]
                idx=0
                for doi in ref_doi:
                    if doi not in unique_doi:
                        unique_doi.append(doi) 
                        prov_list=[]
                        prov_list.append(ref_internal_id[idx])
                        list_ref.append(prov_list)
                        idx+=1
                    else:
                        prov_list.append(ref_internal_id[idx])
                        idx+=1
                for list in list_ref:
                    string_authors=", ".join(list)
                    string_list.append(string_authors)

                ref_list=DataFrame({ "ref_doi":unique_doi, "cites":string_list})   
                ref_list
                
                # Publishers dataframe
                # The dois' keys' values are just dictionaries
                #create a list for each dictionary, crossref, id and name

                pub_cr=[]           
                pub_id=[]
                pub_name=[]

                for key in other_data_pub:
                    pub_val = other_data_pub[key]
                    pub_cr.append(key)
                    pub_id.append(pub_val["id"])
                    pub_name.append(pub_val["name"])

                pub_cr_s=Series(pub_cr)
                pub_id_s=Series(pub_id)
                pub_name_s=Series(pub_name)

                publishers_df=DataFrame({
                    "crossref" : pub_cr_s,
                    "publisher id" : pub_id_s,
                    "name" : pub_name_s,
                })
                publishers_df

                

                from pandas import merge


                publishers_ids = publishers_df[["publisher id", "name"]]

                # Generate a list of internal identifiers for the publishers
                pub_internal_id = []
                for idx, row in publishers_ids.iterrows():
                    pub_internal_id.append("publisher-" + str(idx))

                # Add the list of venues internal identifiers as a new column of the data frame via the class 'Series'
                publishers_ids.insert(0, "pub_id", Series(pub_internal_id, dtype="string"))

                # Show the new data frame on screen
                publishers_ids
                
                #Create a dataframe with all information retrieved from venues
                global_df= merge(unique_venues_ids_df, auth_pub_with_id, left_on="venues doi", right_on="doi")
                
                from sqlite3 import connect

                with connect(self.dbPath) as con:

                    unique_venues_ids_df.to_sql("Venues_json", con, if_exists="append", index=False)
                    person_df.to_sql("Person", con, if_exists ="append", index=False)
                    authors_list.to_sql("Authors_Obj", con, if_exists ="append", index=False)
                    global_df.to_sql("Global_json_dataframe", con, if_exists="append", index= False)
                    auth_pub_with_id.to_sql("Authors and publications", con, if_exists="append", index=False)
                    ref_ids.to_sql("References", con, if_exists="append", index=False)
                    ref_list.to_sql("References_Obj", con, if_exists="append", index=False)
                    publishers_ids.to_sql("Publishers", con, if_exists = "append", index=False)
                    con.commit()

                return True
        else: return False +"Please upload a file with format '.json' or '.csv"

class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self):
        super().__init__()

    def getPublicationsPublishedInYear(self,year):
        with connect(self.dbPath) as con:
            query= """SELECT DISTINCT "id", "title", "publication_year", "venues_intid", "author", "cites"
            FROM "Publications"
            LEFT JOIN Authors_Obj
            ON Publications.id = Authors_Obj.auth_doi
            LEFT JOIN References_Obj
            ON Publications.id = References_Obj.ref_doi
            LEFT JOIN "Venues_json"
            ON "Publications".id="Venues_json".'venues doi'
            WHERE publication_year =?"""
            dframe =read_sql(query,con,params=[year])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue"})
            return dframe

    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:  
            query="""SELECT DISTINCT "id", "title", "publication_year", "venues_intid", "author", "ref_id" 
            FROM "Authors and Publications" 
            LEFT JOIN "Publications" 
            ON "Authors and Publications".doi = Publications.id 
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            LEFT JOIN Authors_Obj
            ON Publications.id = Authors_Obj.auth_doi
            LEFT JOIN "Venues_json"
            ON "Publications".id="Venues_json".'venues doi'
            WHERE orcid =?"""
            dframe=read_sql(query,con, params=[id])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "ref_id":"cites"})
            dframe.astype({"publicationYear":"int"})
            return dframe

    def getMostCitedPublication(self): 
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "id", "title", "publication_year", "venues_intid", "author", "ref_id",   
                    COUNT("citation") AS value_occurrence 
                    FROM "References" LEFT JOIN "Publications" 
                    ON "References".'citation' = "Publications".id
                    LEFT JOIN Authors_Obj
                    ON Publications.id = Authors_Obj.auth_doi
                    LEFT JOIN "Venues_json"
                    ON "Publications".id="Venues_json".'venues doi'
                    WHERE "citation" != "[]"
                    GROUP BY "citation"
                    ORDER BY value_occurrence DESC 
                    """
            dframe =read_sql(query,con)
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "'auth_id'":"author", "ref_id":"cites"})
            dframe.astype({"publicationYear":"int"})
            col_ind=6
            for idx, row in dframe.iterrows():
                if dframe.iloc[idx+1, col_ind]<dframe.iloc[idx, col_ind]:
                    return dframe.iloc[:idx+1]
                                              
    def getMostCitedVenue(self): 
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "issn", "publication_venue", "publisher",  
                    COUNT("publication_venue") AS value_occurrence 
                    FROM "References" 
                    LEFT JOIN "Publications" 
                    ON "References".'citation' = "Publications".id
                    LEFT JOIN "Venues_json"
                    ON "Publications".id="Venues_json".'venues doi'
                    GROUP BY "publication_venue"
                    ORDER BY value_occurrence DESC 
                    """
            dframe =read_sql(query,con)
            dframe=dframe.rename(columns={"publication_venue":"title", "issn":"id"})
            col_ind=3
            for idx, row in dframe.iterrows():
                if dframe.iloc[idx+1, col_ind]<dframe.iloc[idx, col_ind]:
                    return dframe.iloc[:idx+1]

    def getPublicationInVenue(self, venue_id):
        with connect(self.dbPath) as con: 
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "publication_year", "venues_intid", "auth_id", "ref_id"
            FROM Publications 
            LEFT JOIN Global_json_dataframe 
            ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ?"""
            dframe= read_sql(query,con, params=['%'+venue_id+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getVenuesByPublisherId(self, publisher):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publication_venue", "publisher", "issn" 
            FROM Publications
            LEFT JOIN Venues_json
            ON Publications.id=Venues_json.'venues doi'
            WHERE publisher=?"""
            dframe= read_sql(query,con,params=[publisher])
            dframe=dframe.rename(columns={"publication_venue":"title", "issn":"id"})
            return dframe

    def getJournalArticlesInVolume(self, volume, venue_id):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  
            "publication_year", "venues_intid", "auth_id", "ref_id"  FROM Publications 
            LEFT JOIN 
            Global_json_dataframe ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ? AND volume=?"""
            dframe= read_sql(query,con,params=['%'+venue_id+'%',volume])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getJournalArticlesInIssue(self, issue, volume, venue_id):
         with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  "publication_year",
            "venues_intid", "auth_id","ref_id"  FROM "Publications" 
            LEFT JOIN 
            Global_json_dataframe ON Publications.id = Global_json_dataframe.doi
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ? AND volume=? AND issue=? AND type=='journal-article'"""
            dframe= read_sql(query,con,params=[('%'+venue_id+'%'), volume, issue])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe 

    def getJournalArticlesInJournal(self, venue_id):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "issue", "volume",  
            "publication_year", "venues_intid", "auth_id", "ref_id"  
            FROM Publications 
            LEFT JOIN Global_json_dataframe ON Publications.id = Global_json_dataframe.doi 
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            WHERE issn LIKE ? AND type=='journal-article'"""
            dframe=read_sql(query,con,params=['%'+venue_id+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe   
    
    def getProceedingsByEvent(self, event):
        with connect(self.dbPath) as con:
            query="""SELECT DISTINCT "issn", "publication_venue", "publisher", "event" 
            FROM Publications 
            LEFT JOIN Venues_json
            ON Publications.id=Venues_json.'venues doi'
            WHERE event LIKE ?"""
            dframe=read_sql(query, con, params=['%'+event+'%'])
            dframe=dframe.rename(columns={"issn":"id", "publication_venue":"title"})            
            return dframe
    
    def getPublicationAuthors(self, doi):
        with connect(self.dbPath) as con:
            query="SELECT DISTINCT auth_id, family_name, given_name, orcid FROM Person WHERE doi =?"
            dframe=read_sql(query,con,params=[doi])
            dframe=dframe.rename(columns={"orcid":"id", "family_name":"familyName", "given_name":"givenName"})
            return dframe

    def getPublicationsByAuthorName(self, auth_name): #here it is assumed that both given name and family name can be given as input
        with connect(self.dbPath) as con:
            auth_name=auth_name.capitalize()
            query="""SELECT DISTINCT "publicationInternalID", "id", "title", "publication_year", "venues_intid", "auth_id", "ref_id" 
            FROM "Authors and Publications" 
            LEFT JOIN "Publications" 
            ON "Authors and Publications".doi = Publications.id
            LEFT JOIN "References" 
            ON "Publications".id = "References".'ref doi'
            LEFT JOIN Venues_json
            ON Publications.id=Venues_json.'venues doi'
            WHERE family_name LIKE ? OR given_name LIKE ?"""
            dframe= read_sql(query,con,params=['%'+auth_name+'%','%'+auth_name+'%'])
            dframe=dframe.rename(columns={"publication_year":"publicationYear", "venues_intid":"publicationVenue", "auth_id":"author", "ref_id":"cites"})
            return dframe

    def getDistinctPublisherOfPublications(self, doi):
        result=[]
        with connect(self.dbPath) as con:
            for i in doi:
            
                cur= con.cursor()
                query="""SELECT DISTINCT name, publisher from Publishers LEFT JOIN Publications 
                ON Publishers.'publisher id' == Publications.'publisher' WHERE id=?"""
                cur.execute(query,(i,))
                result.append(cur.fetchone())
            if len(result) != 0 and (len(result) == 1 and result[0] != None):
                dframe= DataFrame(result,columns=["name","id"])
            else:
                dframe=DataFrame()
            return dframe
    


# fede_test= RelationalDataProcessor()
# fede_test.setdbPath("testingatt.db")
# fede_test.uploadData("relational_publications.csv")
# fede_test.uploadData("relational_other_data.json")
# # #print(fede_test.getdbPath())
#print(rlp_query.getPublicationsPublishedInYear(2014))
#print(rlp_query.getPublicationsByAuthorId("0000-0003-0530-4305"))
# print(rlp_query.getVenuesByPublisherId("crossref:78"))
# print(rlp_query.getPublicationInVenue("issn:0944-1344"))
# print(rlp_query.getJournalArticlesInIssue("1", "12", "issn:1758-2946"))
# rlp_query.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"])
# rlp_query.getMostCitedPublication()
#print(rel_qp.getJournalArticlesInJournal("issn:2164-551"))