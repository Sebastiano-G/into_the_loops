from GraDBClasses import *
from RelDBClasses import *
from DMClasses import *
from GenPClasses import *

# !! CREATE A RELATIONAL DATABASE !!

#1) 
rel_path = "relational.db"
#rel_dp = RelationalDataProcessor()
#rel_dp.setdbPath(rel_path)

#2) Upload DATA 
#rel_dp.uploadData("relational_publications.csv")
#rel_dp.uploadData("relational_other_data.json")

#3) Instantiate a Relational Query Processor
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)



# !! CREATE A GRAPH DATABASE !!

#1)
grp_endpoint = "http://192.168.1.173:9999/blazegraph/sparql"
grp_dp = TriplestoreDataProcessor()
grp_dp.setEndpointUrl(grp_endpoint)

#2) Upload DATA
#grp_dp.uploadData("graph_other_data.json")
#grp_dp.uploadData("graph_publications.csv")




#3) Instantiate a Triplestore Query Processor
grp_qp = TriplestoreQueryProcessor()
grp_qp.setEndpointUrl(grp_endpoint)


# !! INSTANTIATE A GENERIC QUERY PROCESSOR !!

#1) 
generic = GenericQueryProcessor()

#2) Add the query processors
generic.addQueryProcessor(grp_qp)
generic.addQueryProcessor(rel_qp)



#print(generic.getProceedingsByEvent(""))
print("--------------------------\n", generic.getPublicationsPublishedInYear(2014)) #DONE!
print("--------------------------\n",generic.getPublicationsByAuthorId("0000-0003-0530-4305")) #DONE!
print("--------------------------\n",generic.getVenuesByPublisherId("crossref:98")) #DONE!
print("--------------------------\n",generic.getPublicationInVenue("issn:0138-9130")) #DONE!
print("--------------------------\n",generic.getJournalArticlesInIssue(1, 3, "issn:2052-4463"))   #DONE!   
print("--------------------------\n",generic.getJournalArticlesInVolume(3, "issn:2052-4463"))  #DONE!
print("--------------------------\n",generic.getPublicationAuthors("doi:10.1038/sdata.2016.18")) #DONE!
for el in generic.getPublicationAuthors("doi:10.1038/sdata.2016.18"):
    print(el.getFamilyName())
    print(el.getIds())
print("--------------------------\n",generic.getPublicationsByAuthorName("silvio")) #DONE!
print("--------------------------\n",generic.getDistinctPublisherOfPublications(["doi:10.1007/s00799-019-00266-3"]))
for el in generic.getDistinctPublisherOfPublications(["doi:10.1007/s00799-019-00266-3"]):
    print("--------------------------\n",el.getName())
    print("--------------------------\n",el.getIds())
print("--------------------------\n",generic.getVenuesByPublisherId("crossref:78"))
for el in generic.getVenuesByPublisherId("crossref:78"):
    print("--------------------------\n",el.getIds())
    print("--------------------------\n",el.getTitle())
for el in generic.getVenuesByPublisherId("crossref:8722"):
    print("--------------------------\n",el.getIds())
    print("--------------------------\n",el.getTitle())
print("--------------------------\n",generic.getPublicationInVenue("issn:0944-1344"))
print("--------------------------\n",generic.getJournalArticlesInIssue(1, 2, "issn:1758-2946"))
print("--------------------------\n",generic.getDistinctPublisherOfPublications(["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))

q2 = generic.getMostCitedPublication()
for el in q2.getCitedPublications():
    print("--------------------------\n",el.getIds())
    for element in el.getCitedPublications():
        print("--------------------------\n",element.getIds())

print("--------------------------\n",generic.getMostCitedVenue().getPublisher())
print("--------------------------\n",generic.getMostCitedVenue().getTitle())
print("--------------------------\n",generic.getMostCitedVenue().getIds())
print("--------------------------\n",generic.getMostCitedVenue().getPublisher().getIds())
print("--------------------------\n",generic.getMostCitedVenue().getPublisher().getName())
print("--------------------------\n",generic.getMostCitedPublication().getIds())
print("--------------------------\n",generic.getMostCitedPublication().getPublicationVenue().getIds())
print("--------------------------\n",generic.getMostCitedPublication().getPublicationVenue().getPublisher())
print("--------------------------\n",generic.getMostCitedPublication().getPublicationVenue().getPublisher().getName())
print("--------------------------\n",generic.getMostCitedPublication().getPublicationVenue().getPublisher().getIds())
print("--------------------------\n",generic.getPublicationInVenue("issn:0305-1048"))

print("--------------------------\n",generic.getVenuesByPublisherId("crossref:286"))
for el in generic.getVenuesByPublisherId("crossref:286"):
    print(el)
    print("--------------------------\n",el.getPublisher().getIds()) 

""" 
store = SPARQLUpdateStore()
store.open((grp_endpoint, grp_endpoint))

store.remove((None, None, None), None) """



