from rdflib import Graph
from rdflib import URIRef
from rdflib import Literal
from rdflib import RDF
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from sparql_dataframe import get
from pandas import read_csv
from json import load
from pandas import Series
from pandas import DataFrame
from pandas import merge


class TriplestoreProcessor(object):
    def getEndpointUrl(self):
        return self.endpointUrl
    
    def setEndpointUrl(self, url):
        self.endpointUrl = url
        return True

class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()

    def uploadData(self, path):

        #find the endpoint:
        endpoint = self.getEndpointUrl()

        #is there any statement stored in my endpoint?
        store = SPARQLUpdateStore()
        store.open((endpoint, endpoint))
        triples_number = store.__len__(context=None)   
        store.close()

        my_graph = Graph()

        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        ProceedingsP = URIRef("http://purl.org/spar/fabio/ProceedingsPaper")

        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")
        Proceeding = URIRef("http://purl.org/spar/fabio/AcademicProceedings")

        doi = URIRef("https://schema.org/identifier")
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        chapter_num = URIRef("https://schema.org/numberedPosition")
        publisher = URIRef("https://schema.org/publisher")
        publicationVenue = URIRef("https://schema.org/isPartOf")
        author = URIRef("https://schema.org/author")
        name = URIRef("https://schema.org/givenName")
        surname = URIRef("https://schema.org/familyName")
        citation = URIRef("https://schema.org/citation")
        event = URIRef("https://schema.org/recordedIn")

        #base url for our subjects
        base_url = "https://in.io/res/"

        #CSV file loading:
        if ".csv" in path:
            count = 0
            publ_venues_dict = {}
            publisher_dict = {}
            venues_idx = 0
            publications = read_csv (path, keep_default_na=False,
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
            if triples_number == 0:
                for idx, row in publications.iterrows():
                    local_id = "publication-" + str(idx)

                    
                    # The shape of the new resources that are publications is
                    # 'https://comp-data.github.io/res/publication-<integer>'
                    subj = URIRef(base_url + local_id)

                    #In this first case, the if condition tells us if each of the publications is of type "journal-article"
                    #(look at the dataframe above)
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    
                        # These two statements applies only to journal article:
                        my_graph.add((subj, issue, Literal(row["issue"]))) 
                        my_graph.add((subj, volume, Literal(row["volume"]))) 
                    
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                        my_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    
                    elif row["type"] == "proceedings-paper":
                        my_graph.add((subj, RDF.type, ProceedingsP))
                    
                    
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    my_graph.add((subj, doi, Literal(row["id"])))
                    
                    #how to deal with publication_venues? We check whether they are included or not in the graph: then, we link it to the publication
                    
                    if row["publication_venue"] not in publ_venues_dict and row["publication_venue"]!= "":   
                        venues_id = "venue-" + str(len(publ_venues_dict))
                        venue_subj = URIRef(base_url + venues_id)
                        publ_venues_dict[row["publication_venue"]] = venue_subj
                        my_graph.add((subj, publicationVenue, venue_subj))
                        
                        if row["venue_type"] == "journal": 
                            my_graph.add((venue_subj, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            my_graph.add((venue_subj, RDF.type, Book))
                        elif row["venue_type"] == "proceedings":
                            my_graph.add((venue_subj, RDF.type, Proceeding))
                            my_graph.add((venue_subj, event, Literal(row["event"])))
                        my_graph.add((venue_subj, title, Literal(row["publication_venue"])))

                        
                    elif row["publication_venue"] in publ_venues_dict and row["publication_venue"]!= "":
                        venue_subj = publ_venues_dict[row["publication_venue"]]
                        my_graph.add((subj, publicationVenue, venue_subj))

                    if row["publisher"] not in publisher_dict and row["publisher"]!= "":
                        publisher_subj = URIRef(base_url + "publisher-" + str(len(publisher_dict)))
                        publisher_dict[row["publisher"]] = publisher_subj
                        my_graph.add((venue_subj, publisher, publisher_subj))
                        my_graph.add((publisher_subj, doi, Literal(row["publisher"])))
                    elif row["publisher"] in publisher_dict and row["publisher"]!= "":
                        publisher_subj = publisher_dict[row["publisher"]]
                        my_graph.add((venue_subj, publisher, publisher_subj))

            elif triples_number > 0:
                #I check how many publications have already been introduced in the graph (each of them with a different URI)
                #the regex "doi" allows me to avoid misunderstanding with other types of identifier (for authors, venues, etc.)
                new_query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, new_query, True)
                number_of_publications = result_df.shape[0]

                how_many_venues ="""
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?venue
                WHERE {
                ?publ schema:isPartOf ?venue .
                }
                """
                venues_idx = get(endpoint, how_many_venues, True).shape[0]
                for idx, row in publications.iterrows():
                    df_doi = row["id"]
                    #is the doi in the store?
                    query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi = get(endpoint, query.format(df_doi), True)
                    if check_doi.empty:
                        #it means that no-info about that publication is available on the database
                        subj = URIRef(base_url + "publication-" + str(number_of_publications + count))
                        count +=1

                        #check whether the publication_venue has already been defined in the graph-database:
                        #I'm still inside the check_doi.empty condition, so there is no possibility that some information about that venue  
                        #has been considered, unless the venue also contains another publication which I have already presented in the database.
                        #In other words, different publications from different CSV files, but in both the CSV files they will have the same title.
                        venue_query ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?venue schema:name "{0}" .
                        }}
                        """
                        publications_inside_a_venue = get(endpoint, venue_query.format(row["publication_venue"]), True)
                        if publications_inside_a_venue.empty:
                            if row["publication_venue"] in publ_venues_dict and row["publication_venue"]!="":
                                venue_subj = publ_venues_dict[row["publication_venue"]]
                            elif row["publication_venue"]!="" and row["publication_venue"] in publ_venues_dict:
                            #I create a new URIRef for that specific venue
                                venue_subj = URIRef(base_url + "venue-" + str(venues_idx + len(publ_venues_dict)))
                                publ_venues_dict[row["publication_venue"]] = venue_subj
                                if row["venue_type"] == "journal": 
                                    my_graph.add((venue_subj, RDF.type, Journal))
                                elif row["venue_type"] == "book":
                                    my_graph.add((venue_subj, RDF.type, Book))
                                elif row["venue_type"] == "proceedings":
                                    my_graph.add((venue_subj, RDF.type, Proceeding))
                                    my_graph.add((venue_subj, event, Literal(row["event"])))
                                my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                        else:
                            venue_subj = URIRef(publications_inside_a_venue.at[0, "venue"])
                            if row["venue_type"] == "journal": 
                                    my_graph.add((venue_subj, RDF.type, Journal))
                            elif row["venue_type"] == "book":
                                my_graph.add((venue_subj, RDF.type, Book))
                            my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                        my_graph.add((subj, publicationVenue, venue_subj))
                    else:
                        #the doi is already expressed in the database: what is the URIRef of the specific publication?
                        new_query = """
                        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                        PREFIX schema: <https://schema.org/>

                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}"
                        }}
                        """
                        subj = URIRef(get(endpoint, new_query.format(df_doi), True).at[0, 'publication'])

                        #is the doi already linked to a venue in my database?
                        venue_query ="""
                        PREFIX schema: <https://schema.org/>
                        SELECT ?venue
                        WHERE {{
                            ?publication schema:isPartOf ?venue .
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        venues_and_doi = get(endpoint, venue_query.format(df_doi), True)
                        if venues_and_doi.empty:
                            #the publication is not associated with a venue
                            if row["publication_venue"] in publ_venues_dict and row["publication_venue"]!="":
                                venue_subj = publ_venues_dict[row["publication_venue"]]
                            elif row["publication_venue"] not in publ_venues_dict and row["publication_venue"]!="":
                                venue_query ="""
                                PREFIX schema: <https://schema.org/>
                                SELECT ?venue
                                WHERE {{
                                    ?publication schema:isPartOf ?venue .
                                    ?venue schema:name "{0}" .
                                }}
                                """
                                check_venue = get(endpoint, venue_query.format(row["publication_venue"]), True)
                                if check_venue.empty:
                                    #I create a new URIRef for that specific venue
                                    venue_subj = URIRef(base_url + "venue-" + str(venues_idx + len(publ_venues_dict)))
                                    publ_venues_dict[row["publication_venue"]] = venue_subj
                                    if row["venue_type"] == "journal": 
                                        my_graph.add((venue_subj, RDF.type, Journal))
                                    elif row["venue_type"] == "book":
                                        my_graph.add((venue_subj, RDF.type, Book))
                                    my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                                else:
                                    venue_subj = URIRef(check_venue.at[0, 'venue'])
                                    
                        else: 
                            venue_subj = URIRef(venues_and_doi.at[0, 'venue'])

                        if row["venue_type"] == "journal": 
                            my_graph.add((venue_subj, RDF.type, Journal))
                        elif row["venue_type"] == "book":
                            my_graph.add((venue_subj, RDF.type, Book))
                        if row["publication_venue"]!="":
                            my_graph.add((venue_subj, title, Literal(row["publication_venue"])))
                            my_graph.add((subj, publicationVenue, venue_subj))
                        
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    
                        # These two statements applies only to journal article:
                        my_graph.add((subj, issue, Literal(row["issue"]))) 
                        my_graph.add((subj, volume, Literal(row["volume"]))) 
                    
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                        my_graph.add((subj, chapter_num, Literal(row["chapter"])))
                    
                    elif row["type"] == "proceedings-paper":
                        my_graph.add((subj, RDF.type, ProceedingsP))
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    my_graph.add((subj, doi, Literal(df_doi)))
                    
                    if row["publisher"]!="":                    
                        publisher_check="""
                        PREFIX schema:<https://schema.org/>
                        SELECT ?publisher
                        WHERE {{
                            ?publisher schema:identifier "{0}"
                        }}
                        """
                        publisher_check_df = get(endpoint, publisher_check.format(row["publisher"]), True)
                        if publisher_check_df.empty:
                            if row["publisher"] in publisher_dict:
                                publisher_subj = publisher_dict[row["publisher"]]
                                my_graph.add((venue_subj, publisher, publisher_subj))
                            else:
                                how_many_publishers = """
                                PREFIX schema:<https://schema.org/>
                                SELECT ?publisher
                                WHERE {
                                    ?publisher schema:identifier ?pub_id .
                                    FILTER regex(?pub_id, "crossref")
                                }
                                """
                                how_many_publishers_idx = get(endpoint, how_many_publishers, True).shape[0]
                                publisher_subj = URIRef(base_url + "publisher-" + str(len(publisher_dict) + how_many_publishers_idx))
                                my_graph.add((venue_subj, publisher, publisher_subj))
                                my_graph.add((publisher_subj, doi, Literal(row["publisher"])))
                                publisher_dict[row["publisher"]] = publisher_subj
                        else:
                            publisher_subj = URIRef(publisher_check_df.at[0, "publisher"])
                            my_graph.add((venue_subj, publisher, publisher_subj))




        #JSON file loading:
        elif ".json" in path:
            with open(path, "r", encoding="utf-8") as f:
                graph_other_data = load(f)

            graph_other_data_authors = graph_other_data.get("authors")
            graph_other_data_venues = graph_other_data.get("venues_id")
            graph_other_data_ref = graph_other_data.get("references")
            graph_other_data_pub = graph_other_data.get("publishers")

            #authors
            auth_doi=[]
            auth_fam=[]
            auth_giv=[]
            auth_id=[]

            for key in graph_other_data_authors:
                a=[]
                b=[]
                c=[]
                auth_val = graph_other_data_authors[key]
                auth_doi.append(key)
                for dict in auth_val:
                    a.append(dict["family"])
                    b.append(dict["given"])
                    c.append(dict["orcid"])
                auth_fam.append(a)
                auth_giv.append(b)
                auth_id.append(c)

                auth_doi_s=Series(auth_doi)
            auth_fam_s=Series(auth_fam)
            auth_giv_s=Series(auth_giv)
            auth_id_s=Series(auth_id)

            authors_df=DataFrame({
                "auth doi" : auth_doi_s,
                "family name" : auth_fam_s, 
                "given name" : auth_giv_s, 
                "orcid" : auth_id_s
            })

            #venues
            venues_doi=[]
            venues_issn=[]

            for key in graph_other_data_venues:
                venues_val = graph_other_data_venues[key]
                venues_doi.append(key)
                venues_issn.append(venues_val)

            unique_venues=[]
            list_dois=[]

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

            venues_ids = unique_venues_df[["venues doi", "issn"]]
            venues_internal_id = []
            for idx, row in venues_ids.iterrows():
                venues_internal_id.append("venues-" + str(idx))
            venues_ids.insert(0, "venues id", Series(venues_internal_id, dtype="string"))
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
                for list in outlist:
                    for dois in list:
                        l_doi.append(dois)
                        l_intid.append(venues_id_intid[i][0])
                        l_issn.append(venues_id_issn[i][0])
            s_intid=Series(l_intid)
            s_doi=Series(l_doi)
            s_issn=Series(l_issn)
            unique_venues_ids_df=DataFrame({"venues_intid":s_intid, "venues doi":s_doi, "issn":s_issn})

            #references
            ref_doi=[]
            ref_cit=[]

            for key in graph_other_data_ref:
                ref_val = graph_other_data_ref[key]
                ref_doi.append(key)
                ref_cit.append(ref_val)

            ref_doi_s=Series(ref_doi)
            ref_cit_s=Series(ref_cit)

            ref_df=DataFrame({
                "ref doi" : ref_doi_s,
                "citation" : ref_cit_s, 
            })

            #publishers
            pub_cr=[]
            pub_id=[]
            pub_name=[]

            for key in graph_other_data_pub:
                pub_val = graph_other_data_pub[key]
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

            venues_authors = merge(unique_venues_ids_df, authors_df, left_on="venues doi", right_on="auth doi", how="outer")
            venues_authors_ref = merge(venues_authors, ref_df, left_on="venues doi", right_on="ref doi", how="outer")

            authors_dict = {}
            publications_possibly_reused = {}
            a_list = [] #I need this list to create a type comparison
            venues_dict = {}
            publisher_dict = {}

            if triples_number == 0:
                for idx, row in venues_authors_ref.iterrows():
                    if type(row["orcid"]) == type(a_list):
                        for i in range(len(row["orcid"])):
                            if row["auth doi"] in publications_possibly_reused:
                                subj = publications_possibly_reused[row["auth doi"]]
                            else:
                                subj = URIRef(base_url + "publication-" + str(len(publications_possibly_reused)))
                                publications_possibly_reused[row["auth doi"]] = subj
                            if row["orcid"][i] not in authors_dict:
                                author_subj = URIRef(base_url + "author-" + str(len(authors_dict)))
                                authors_dict[row["orcid"][i]] = author_subj
                                my_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                my_graph.add((author_subj, name, Literal(row["given name"][i])))
                                my_graph.add((author_subj, surname, Literal(row["family name"][i])))
                            else: 
                                author_subj = authors_dict[row["orcid"][i]]
                            my_graph.add((subj, doi, Literal(row["auth doi"])))
                            my_graph.add((subj, author, author_subj))

                    #venues
                    #check if the publication is already associated with a URIRef:
                    if type(row["issn"]) == type(a_list) and len(row["issn"]) >= 1:
                        #is the publication associated with a venue URIRef?
                        #list_to_string is used to avoid the TypeError: Unhashable type 'list'
                        if row["venues doi"] in publications_possibly_reused:
                            a_subj = publications_possibly_reused[row["venues doi"]]
                        else:
                            new_idx = len(publications_possibly_reused)
                            a_subj = URIRef(base_url + "publication-" + str(new_idx))
                            publications_possibly_reused[row["venues doi"]] = a_subj
                        list_to_string = ' '.join(row["issn"])
                        if list_to_string in venues_dict: 
                            venue_subj = venues_dict[list_to_string]
                        else:
                            venue_subj = URIRef(base_url + "venue-" + str(len(venues_dict))) 
                            venues_dict[list_to_string] = venue_subj
                            for j in range(len(row["issn"])):
                                my_graph.add((venue_subj, doi, Literal(row["issn"][j])))  
                        my_graph.add((a_subj, publicationVenue, venue_subj))
                                
                    
                    
                    #citations
                    if type(row["citation"]) == type(a_list):
                        if row["ref doi"] in publications_possibly_reused:
                            subj = publications_possibly_reused[row["ref doi"]]
                        else: 
                            new_idx = len(publications_possibly_reused)
                            subj = URIRef(base_url + "publication-" + str(new_idx))
                            my_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_possibly_reused[row["ref doi"]] = subj
                        for w in range(len(row["citation"])):
                            #have I already created a URIRef for the cited publication?
                            if row["citation"][w] in publications_possibly_reused:
                                cited_publ = publications_possibly_reused[row["citation"][w]]
                            else:
                                extra_idx = len(publications_possibly_reused)
                                cited_publ = URIRef(base_url + "publication-" + str(extra_idx))
                                publications_possibly_reused[row["citation"][w]] = cited_publ
                                my_graph.add((cited_publ, doi, Literal(row["citation"][w])))
                            my_graph.add((subj, citation, cited_publ))
                
                for idx, row in publishers_df.iterrows():
                    subj = URIRef(base_url + "publisher-" + str(idx))
                    my_graph.add((subj, doi, Literal(row["crossref"])))
                    my_graph.add((subj, title, Literal(row["name"])))        
                
            elif triples_number > 0:
                #how many authors have already been introduced in the db?
                authors_in_db = """
                PREFIX schema: <https://schema.org/>

                SELECT DISTINCT ?author
                WHERE {
                    ?publication schema:author ?author .
                }
                """
                authors_in_db_df = (get(endpoint, authors_in_db, True)).shape[0]
                venues_in_db = """
                PREFIX schema: <https://schema.org/>

                SELECT DISTINCT ?venue
                WHERE {
                    ?publication schema:isPartOf ?venue .
                }
                """
                venues_in_db_df = (get(endpoint, venues_in_db, True)).shape[0]

                new_query = """
                PREFIX schema: <https://schema.org/>
                SELECT DISTINCT ?publication
                WHERE {
                    ?publication schema:identifier ?identifier .
                    FILTER regex(?identifier, "doi")
                }
                """
                result_df = get(endpoint, new_query, True)
                number_of_publications = result_df.shape[0]
                for idx, row in venues_authors_ref.iterrows():
                    #check for each of the dois if they exist in the database:
                    #authors
                    if type(row["orcid"]) == type(a_list):
                        query = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publication
                        WHERE {{
                            ?publication schema:identifier "{0}" .
                        }}
                        """
                        check_doi = get(endpoint, query.format(row["auth doi"]), True)
                        if check_doi.empty:
                            if row["auth doi"] in publications_possibly_reused:
                                subj = publications_possibly_reused[row["auth doi"]]
                            else:
                                number_of_index = number_of_publications + len(publications_possibly_reused)
                                subj = URIRef(base_url + "publication-" + str(number_of_index))
                                publications_possibly_reused[row["auth doi"]] = subj
                            my_graph.add((subj, doi, Literal(row["auth doi"])))
                        else:
                            subj = URIRef(check_doi.at[0, "publication"])                 
                        for i in range(len(row["orcid"])):
                            auth_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?author
                            WHERE {{
                                ?publication schema:author ?author .
                                ?author schema:identifier "{0}"
                            }}
                            """
                            check_auth = get(endpoint, auth_query.format(row["orcid"][i]), True)   
                            if check_auth.empty:                         
                                if row["orcid"][i] not in authors_dict:
                                    author_subj = URIRef(base_url + "author-" + str(len(authors_dict) + authors_in_db_df))
                                    authors_dict[row["orcid"][i]] = author_subj
                                    my_graph.add((author_subj, doi, Literal(row["orcid"][i])))
                                    my_graph.add((author_subj, name, Literal(row["given name"][i])))
                                    my_graph.add((author_subj, surname, Literal(row["family name"][i])))
                                else: 
                                    author_subj = authors_dict[row["orcid"][i]]
                            else:
                                author_subj = URIRef(check_auth.at[0, "author"])
                            my_graph.add((subj, author, author_subj))
                    #venues
                    query_two = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi_two = get(endpoint, query_two.format(row["venues doi"]), True)
                    if check_doi_two.empty:
                        if row["venues doi"] in publications_possibly_reused:
                            subj = publications_possibly_reused[row["venues doi"]]
                        else:
                            number_of_index = number_of_publications + len(publications_possibly_reused)
                            subj = URIRef(base_url + "publication-" + str(number_of_index))
                            publications_possibly_reused[row["venues doi"]] = subj
                    else:
                        subj = URIRef(check_doi_two.at[0, "publication"])
                    #I have a subj (a publication): now I need to check if the subj is already associated with a venue
                    if type(row["issn"]) == type(a_list):
                        for i in range(len(row["issn"])):
                            venue_query = """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?venue
                            WHERE {{
                                <{0}> schema:isPartOf ?venue .
                            }}
                            """
                            check_venue = get(endpoint, venue_query.format(subj), True)
                            if check_venue.empty:
                                list_to_string = ' '.join(row["issn"])
                                if list_to_string in venues_dict: #I use only the first element of the list in order to avoid the TypeError: unhashable
                                    venue_subj = venues_dict[list_to_string]
                                    my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                                else:
                                    venue_subj = URIRef(base_url + "venue-" + str(venues_in_db_df +len(venues_dict)))
                                    venues_dict[list_to_string] = venue_subj
                                    my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                                my_graph.add((subj, publicationVenue, venue_subj))
                            else:
                                venue_subj = URIRef(check_venue.at[0, "venue"])
                                my_graph.add((venue_subj, doi, Literal(row["issn"][i])))
                    
                    #citations:
                    query_three= """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publication
                    WHERE {{
                        ?publication schema:identifier "{0}" .
                    }}
                    """
                    check_doi_three = get(endpoint, query_three.format(row["ref doi"]), True)
                    if check_doi_three.empty:
                        if row["ref doi"] in publications_possibly_reused:
                            subj = publications_possibly_reused[row["ref doi"]] 
                        else: 
                            new_idx = len(publications_possibly_reused)
                            subj = URIRef(base_url + "publication-" + str(new_idx + number_of_publications))
                            my_graph.add((subj, doi, Literal(row["ref doi"])))
                            publications_possibly_reused[row["ref doi"]] = subj
                    else: 
                        subj = URIRef(check_doi_three.at[0, 'publication'])
                    if type(row["citation"]) == type(a_list):
                        for w in range(len(row["citation"])):
                            #have I already created a URIRef for the cited publication?
                            query_four= """
                            PREFIX schema: <https://schema.org/>
                            SELECT ?publication
                            WHERE {{
                                ?publication schema:identifier "{0}" .
                            }}
                            """
                            check_doi_four = get(endpoint, query_four.format(row["citation"][w]), True)
                            if check_doi_four.empty:
                                if row["citation"][w] in publications_possibly_reused:
                                    cited_publ = publications_possibly_reused[row["citation"][w]]
                                else:
                                    n_index = number_of_publications + len(publications_possibly_reused)
                                    cited_publ = URIRef(base_url + "publication-" + str(n_index))
                                    my_graph.add((cited_publ, doi, Literal(row["citation"][w])))
                                    publications_possibly_reused[row["citation"][w]] = cited_publ
                            else:
                                cited_publ = URIRef(check_doi_four.at[0, "publication"])
                            my_graph.add((subj, citation, cited_publ))
                for idx, row in publishers_df.iterrows():
                    pub_query = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?publisher
                    WHERE {{
                        ?publisher schema:identifier "{0}" .
                    }}
                    """
                    check_pub = get(endpoint, pub_query.format(row["crossref"]), True)
                    if check_pub.empty:
                        num_publisher = """
                        PREFIX schema: <https://schema.org/>
                        SELECT ?publisher
                        WHERE {
                            ?publication schema:publisher ?publisher .
                        }
                        """
                        pub_idx = get(endpoint, num_publisher, True).shape[0]
                        subj = URIRef(base_url + "publisher-" + str(pub_idx + len(publisher_dict)))
                        my_graph.add((subj, doi, Literal(row["crossref"])))
                        my_graph.add((subj, title, Literal(row["name"])))
                        publisher_dict[row["crossref"]] = subj
                    else:
                        subj = URIRef(check_pub.at[0, 'publisher'])
                        my_graph.add((subj, title, Literal(row["name"])))                    
        store.open((endpoint, endpoint))
        for triple in my_graph.triples((None, None, None)):
            store.add(triple)
        store.close()
                
class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self) -> None:
        super().__init__()
    
    #first method: we want to retrieve a DataFrame containing all the publications produced in a certain year, given in input.
    def getPublicationsPublishedInYear(self, year):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?publicationInternalId ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publicationInternalId schema:name ?title .
            ?publicationInternalId schema:identifier ?id .
            ?publicationInternalId schema:datePublished ?publicationYear .
            FILTER (?publicationYear = {0}) .
            OPTIONAL {{?publicationInternalId schema:isPartOf ?publicationVenue .}}
            OPTIONAL {{?publicationInternalId schema:author ?author }} .
            OPTIONAL {{?publicationInternalId schema:citation ?cites}}
        }}
        """

        publ = get(endpoint, new_query.format(year), True)
        return publ
    
    
    def getPublicationsByAuthorId(self, orcid):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:author ?author .
            ?author schema:identifier ?orcid .
            FILTER (?orcid = "{0}") .
        }}
        """
        publ = get(endpoint, new_query.format(orcid), True)
        a_string = ""
        len_publ = publ.shape[0]
        if len_publ > 0: 
            for idx, row in publ.iterrows():
                if idx == 0:
                    a_string = a_string + "(?id = '" + row["id"] + "')"
                else:
                    a_string = a_string + "|| (?id ='" + row["id"] + "')"
        else:
            return publ
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0})
            ?publication schema:author ?author .
            OPTIONAL {{?publication schema:citation ?cites}}
            OPTIONAL {{?publication schema:datePublished ?publicationYear}}
            OPTIONAL {{?publication schema:name ?title}}
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}}
        }}
        """
        new_publ = get(endpoint, second_query.format(a_string), True)    
        return new_publ     

        
    def getMostCitedPublication(self):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?citation (COUNT(?citation) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        }
        GROUP BY ?citation
        ORDER BY desc(?cited)
        """
        publ = get(endpoint, new_query, True)
        a_list = []
        a_string = ""
        if publ.shape[0] > 1:
            a = 0
            b = 1
            most_cited_value = publ.at[0, "cited"]
            a_list.append(publ.at[a, "citation"])
            if publ.at[a, "cited"] == publ.at[b, "cited"]:
                while publ.at[a, "cited"] == publ.at[b, "cited"]:
                    a_list.append(publ.at[b, "citation"])
                    a +=1
                    b +=1
        if len(a_list) == 1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?publication =<" + a_list[n+1] + ">)"
        else:
            return publ
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0}) .
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}} .
            OPTIONAL {{?publication schema:name ?title }} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear }}.
            OPTIONAL {{?publication schema:citation ?cites}}.
            OPTIONAL {{?publication schema:author ?author}}
        }}
        """
        publ_df = get(endpoint, second_query.format(a_string), True)
        publ_df.insert(6, "value_occurrence", most_cited_value)
        return publ_df

    def getMostCitedVenue(self):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?venue (COUNT(?venue) AS ?cited) 
        WHERE { 
        ?publication schema:citation ?citation .
        ?citation schema:isPartOf ?venue
        }
        GROUP BY ?venue
        ORDER BY desc(?cited) 
        """
        venues = get(endpoint, new_query, True)
        a_list = []
        a_string = ""
        if venues.shape[0] > 1:
            a = 0
            b = 1
            a_list.append(venues.at[a, "venue"])
            most_cited_value = venues.at[a, "cited"]
            if venues.at[a, "cited"] == venues.at[b, "cited"]:
                while venues.at[a, "cited"] == venues.at[b, "cited"]:
                    a_list.append(venues.at[b, "venue"])
                    a +=1
                    b +=1
        if len(a_list) == 1:
            a_string = a_string + "(?venue = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?venue = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?venue =<" + a_list[n+1] + ">)"
        else:
            return venues
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publisher
        WHERE {{
            ?venue schema:identifier ?id .
            FILTER ({0})
            OPTIONAL {{?venue schema:name ?title }}
            OPTIONAL {{?venue schema:publisher ?publish .
            ?publish schema:identifier ?publisher}}    
        }}
        """
        venue_df = get(endpoint, second_query.format(a_string), True)
        venue_df.insert(3, "value_occurrence", most_cited_value)
        return venue_df

    
    def getVenuesByPublisherId(self, pub_id):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?title ?publisher
        WHERE {{
            ?venue schema:publisher ?publi .
            ?publi schema:identifier ?publisher .
            FILTER (?publisher = "{0}") . 
            OPTIONAL {{?venue schema:identifier ?id }}.
            OPTIONAL {{?venue schema:name ?title }}.
        }}
        """
        publ = get(endpoint, new_query.format(pub_id), True)
        return publ
    
    def getPublicationInVenue(self, venue):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?publication ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            OPTIONAL {{?publication schema:name ?title}} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear}} .
            OPTIONAL {{?publication schema:author ?author}} .
            OPTIONAL {{?publication schema:citation ?cites}}  .
        }}  
        """
        publ = get(endpoint, new_query.format(venue), True)
        return publ

    def getJournalArticlesInIssue (self, issue, volume ,issn):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            ?publication schema:volumeNumber ?volume .
            FILTER (?volume = "{1}").
            ?publication schema:issueNumber ?issue .
            FILTER (?issue = "{2}") . 
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume, issue), True)
        return publ
    
    def getJournalArticlesInVolume(self, volume, issn):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            ?publication schema:volumeNumber ?volume .
            FILTER (?volume = "{1}").
            OPTIONAL {{?publication schema:issueNumber ?issue}}.
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn, volume), True)
        return publ
    
    def getJournalArticlesInJournal(self, issn):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites ?volume ?issue
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:isPartOf ?publicationVenue .
            ?publicationVenue schema:identifier ?issn .
            FILTER (?issn = "{0}") .
            OPTIONAL {{?publication schema:issueNumber ?issue }}.
            OPTIONAL {{?publication schema:volumeNumber ?volume }}.
            OPTIONAL {{?publication schema:name ?title }}.
            OPTIONAL {{?publication schema:datePublished ?publicationYear .}}
            OPTIONAL {{?publication schema:author ?author}}
            OPTIONAL {{?publication schema:citation ?cites .}}
        }}  
        """
        publ = get(endpoint, new_query.format(issn), True)
        return publ

    def getPublicationAuthors(self, doi):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT ?id ?givenName ?familyName
        WHERE {{
            ?publication schema:identifier "{0}" .
            ?publication schema:author ?author .
            ?author schema:identifier ?id .
            ?author schema:givenName ?givenName .
            ?author schema:familyName ?familyName
        }}
        """
        authors = get(endpoint, new_query.format(doi), True)
        return authors
    
    def getPublicationsByAuthorName(self, name):
        endpoint = self.getEndpointUrl()
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?publication ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            ?publication schema:author ?author .
            ?author schema:givenName ?name .
            ?author schema:familyName ?surname .
            FILTER (regex(?name, "{0}", "i") || regex(?surname, "{1}", "i")).
            OPTIONAL {{?publication schema:name ?title}}.
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}}
            OPTIONAL {{?publication schema:citation ?cites}}
            OPTIONAL {{?publication schema:author ?author}}
        }} 
        """
        publ = get(endpoint, new_query.format(name, name), True)
        a_list =[]
        a_string= ""
        for n in range(publ.shape[0]):
            a_list.append(publ.at[n, "publication"])
        if len(a_list) == 1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
        elif len(a_list)>1:
            a_string = a_string + "(?publication = <" + a_list[0] + ">)"
            for n in range(len(a_list)-1):
                a_string = a_string + "|| (?publication =<" + a_list[n+1] + ">)"
        else:
            return publ
    
        second_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT ?id ?title ?publicationYear ?publicationVenue ?author ?cites
        WHERE {{
            ?publication schema:identifier ?id .
            FILTER ({0}) .
            OPTIONAL {{?publication schema:isPartOf ?publicationVenue}} .
            OPTIONAL {{?publication schema:name ?title }} .
            OPTIONAL {{?publication schema:datePublished ?publicationYear }}.
            OPTIONAL {{?publication schema:citation ?cites}}.
            OPTIONAL {{?publication schema:author ?author}}
        }}
        """
        publ_df = get(endpoint, second_query.format(a_string), True)
        return publ_df

    def getProceedingsByEvent(self, eventPartialName):
        endpoint = self.getEndpointUrl()
        query = """
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?id ?title ?publisher ?event
        WHERE {{
            ?publication schema:isPartOf ?venue .
            ?venue schema:name ?title .
            ?venue schema:recordedIn ?event .
            FILTER regex(?event, "{0}", "i")
            OPTIONAL {{?venue schema:identifier ?id .}}
            OPTIONAL {{?venue schema:publisher ?publi .
            ?publi schema:identifier ?publisher}}
        }}
        """
        venues = get(endpoint, query.format(eventPartialName), True)
        return venues
    
    def getDistinctPublisherOfPublications(self, list_of_pub):
        endpoint = self.getEndpointUrl()
        a_string = ""
        if len(list_of_pub) == 1:
            a_string = a_string + "(?doi = '" + list_of_pub[0] + "')"
        elif len(list_of_pub)>1:
            a_string = a_string + "(?doi = '" + list_of_pub[0] + "')"
            for n in range(len(list_of_pub)-1):
                a_string = a_string + "|| (?doi ='" + list_of_pub[n+1] + "')"
        new_query = """
        PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX schema: <https://schema.org/>

        SELECT DISTINCT ?publisher ?id ?name
        WHERE {{
            ?publication schema:isPartOf ?venue .
            ?venue schema:publisher ?publisher .
            ?publisher schema:identifier ?id .
            ?publication schema:identifier ?doi .
            FILTER ({0}) .
            OPTIONAL {{?publisher schema:name ?name}}
        }} 
        """
        publ = get(endpoint, new_query.format(a_string), True)
        return publ



#grp_dp.uploadData("graph_other_data.json")
#grp_dp.uploadData("graph_publications.csv")
#grp_dp.uploadData("relational_other_data.json")
#grp_dp.uploadData("relational_publications.csv")








#methods queryprocessor!!!!!!!


#print(grp_qp.getProceedingsByEvent(""))
#print(grp_qp.getPublicationsPublishedInYear(2014)) #DONE!
#print(grp_qp.getPublicationsByAuthorId("0000-0003-0530-4305")) #DONE!
#print(grp_qp.getMostCitedPublication()) #DONE!
#print(grp_qp.getMostCitedVenue()) #DONE!
#print(grp_qp.getVenuesByPublisherId("crossref:98")) #DONE!
#print(grp_qp.getPublicationInVenue("issn:0138-9130")) #DONE!
#print(grp_qp.getJournalArticlesInIssue("issn:2052-4463", 3, 1))   #DONE!   
#print(grp_qp.getJournalArticlesInVolume("issn:2052-4463", 3))  #DONE!
#print(grp_qp.getPublicationAuthors("doi:10.1038/sdata.2016.18")) #DONE!
#print(grp_qp.getPublicationsByAuthorName("silvio")) #DONE!
#print(grp_qp.getDistinctPublisherOfPublications(["doi:10.1007/s00799-019-00266-3"])) #DONE!