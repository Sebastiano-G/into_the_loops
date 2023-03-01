from GraDBClasses import *
from RelDBClasses import *
from optparse import Values
import sqlite3
from pandas import read_csv
from sqlite3 import connect
from pandas import read_sql, DataFrame, merge, concat


class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

    def getIds(self): #get a list of the ids of the class
        if type(self.id) == type(list()):
            return self.id
        else:
            return [self.id]

class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):
        self.givenName = givenName
        self.familyName = familyName
        super().__init__(id)   #IMPORTANT: Inherit the parameters/methods/EVERYTHING of the superclass

    def getGivenName(self):  
        return self.givenName   #IMPORTANT: remember return self.<parameter>
  
    def getFamilyName(self):
        return self.familyName


class Publication(IdentifiableEntity):
    def __init__(self, id, author, publicationYear, title, publicationVenue, cites):
        self.author = author #da sistemare
        self.publicationYear = publicationYear
        self.title = title
        self.publicationVenue = publicationVenue
        self.cites = cites #da sistemare

        super().__init__(id)
    
    
    def getAuthors(self):
        return self.author
    
    def getPublicationYear(self):
        return self.publicationYear
    
    def getTitle(self):
        return self.title
        
    def getCitedPublications(self):
        return self.cites
    
    def getPublicationVenue(self):
        return self.publicationVenue

 #***VENUES***   

class Venue(IdentifiableEntity):
    def __init__(self, id, title, publisher):
        self.title = title 
        self.publisher = publisher

        super().__init__(id)
    
    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher

class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def getName(self):
        return self.name


class JournalArticle(Publication): #it's a type of publication with 2 extras
    def __init__(self, id, author, title, publicationYear, publicationVenue, cites, issue, volume):
        self.issue = issue
        self.volume = volume
        # IMPORTANT: Here is where the constructor of the superclass is explicitly recalled, so as
        # to handle the input parameters as done in the superclass
        super().__init__(id, author, title, publicationYear, publicationVenue, cites)  
    
    def getIssue(self):
        return self.issue
    
    def getVolume(self):
        return self.volume


class BookChapter(Publication): #it is a publication that just inherits its parameters
    def __init__(self, id, author, publicationYear, title, publicationVenue, cites, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, author, publicationYear, title, publicationVenue, cites)  

    def getChapterNumber(self):
        return self.chapterNumber

class ProceedingsPaper(Publication):
    pass


class Journal(Venue):
    pass


class Book(Venue):
    pass

class Proceedings(Venue):
    def __init__(self, id, title, publisher, event):
        self.event = event
        super().__init__(id, title, publisher)  
  

    def getEvent(self):
        return self.event