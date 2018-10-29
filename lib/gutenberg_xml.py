import os
import csv
import logging
from itertools import repeat
from lxml import etree

from helpers.xml import xmlParser

class gutenbergXML(xmlParser):

    def __init__(self):
        super(gutenbergXML, self).__init__()

        self.logger = logging.getLogger('guten_logs')

        # Necessary for lxml to properly parse namespaced elements and attribs
        self.loadNamespaces([
            ("base", "http://www.gutenberg.org/"),
            ("cc", "http://web.resource.org/cc/"),
            ("dcam", "http://purl.org/dc/dcam/"),
            ("rdfs", "http://www.w3.org/2000/01/rdf-schema#"),
            ("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#"),
            ("dcterms", "http://purl.org/dc/terms/"),
            ("pgterms", "http://www.gutenberg.org/2009/pgterms/"),
            ("marcrel", "http://id.loc.gov/vocabulary/relators/"),
            ("bibframe", "http://bibframe.org/vocab/"),
            ("drm", "http://librarysimplified.org/terms/drm"),
            ("app", "http://www.w3.org/2007/app"),
            ("bib", "http://bib.schema.org/"),
            ("odps", "http://opds-spec.org/2010/catalog"),
            ("opf", "http://www.idpf.org/2007/opf"),
            ("simplified", "http://librarysimplified.org/terms/"),
            ("schema", "http://schema.org/"),
            (None, "http://www.w3.org/2005/Atom")
        ])

        # Standalone fields we get from the Gutenberg RDF file
        self.fields = [
            ("dcterms", "publisher"),
            ("dcterms", "title"),
            ("dcterms", "rights"),
            ("pgterms", "marc010"),
            ("dcterms", "issued")
        ]

        # Fields we load from entity sections
        self.entity_fields = [
            ("pgterms", "birthdate"),
            ("pgterms", "deathdate"),
            ("pgterms", "name"),
            ("pgterms", "alias")
        ]

        self.relCodes = self._loadLCRels()

        self.metadata = {
            "entities": [],
            "subjects": [],
            "language": None
        }
        self.ebookURLs = []

    def load(self, rdfFile):
        self.logger.debug("Loading data from {}".format(rdfFile))
        self.parse(rdfFile)
        self.loadRecord("pgterms", "ebook") # This resets the XML file context

        self.logger.debug("Creating record from {}".format(rdfFile))
        self._getMetadata()
        self.loadRecord("pgterms", "ebook")

        self.logger.debug("Downloading Ebooks from Gutenberg for {}".format(rdfFile))
        self._getEbooks()
        self.loadRecord("pgterms", "ebook")

        return self.metadata, self.ebookURLs

    # This loads metadata from the Gutenberg RDF files, inluding repeating
    # fields such as subjects and entities (authors, editors, etc)
    def _getMetadata(self):

        self.logger.debug("Storing basic fields and edition data")
        fieldData = self.getFields(self.fields)
        self._storeFields(fieldData, self.metadata)
        self._createEditions()

        self.logger.debug("Storing creator and other contributors")
        self._createEntityRecord(("dcterms", "creator"))
        # This scans the RDF file for all possible marcrel codes
        # and stores the resulting entity records
        for key, value in self.relCodes.items():
            self.loadRecord("pgterms", "ebook")
            self._createEntityRecord(("marcrel", key))

        self.loadRecord("pgterms", "ebook")
        self.logger.debug("Storing subjects from Gutenberg record")
        self._getSubjects()

    def _storeFields(self, fields, obj):
        list(map(self._storeField, fields, repeat(obj)))

    def _storeField(self, field, obj):
        key, value = field
        if value is not None:
            value = value.text
        obj[key] = value

    # This loads the ePub URLs and descriptive data. It is important to grab
    # The size/date modified since we will check those to see if we need to
    # update the files on our end
    def _getEbooks(self):
        formats = self.getRepeatingField("dcterms", "hasFormat")
        urls = list(filter(lambda x: x, map(self._loadEbook, formats)))
        self.ebookURLs = urls

    def _loadEbook(self, format):
        self.current = format
        fileTag, file = self.getField(("pgterms", "file"))
        urlAttrib = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
        url = self.getAttrib(file, urlAttrib)
        if '.epub' in url:
            updatedTag, updated = self.getField(("dcterms", "modified"))
            sizeTag, size = self.getField(("dcterms", "extent"))
            epub = {
                "url": url,
                "size": size.text,
                "updated": updated.text
            }
            return epub
        return False

    # Entity records all share fields in the RDF files, so this grabs all
    # available data from the record and stashes it in a list of dicts
    def _createEntityRecord(self, tags):
        entityDict = {
            "viaf": None,
            "lcnaf": None,
            "sort_name": None,
            "wikipedia": None
        }
        entityTag, entity = self.getField(tags)
        if entity is None:
            return False

        rel = entityTag
        self.logger.debug("Creating entity for with relationship {}".format(rel))
        if entityTag in self.relCodes:
            rel = self.relCodes[entityTag]
        entityDict["role"] = rel
        self.current = entity

        agentTag, agent = self.getField(("pgterms", "agent"))

        aboutAttrib = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about"
        gutenbergAgentID = self.getAttrib(agent, aboutAttrib)
        entityDict["gutenberg_id"] = gutenbergAgentID

        # TODO Handle Multiple aliases
        entityData = self.getFields(self.entity_fields)
        self._storeFields(entityData, entityDict)
        wikipediaTag, wikipedia = self.getField(("pgterms", "webpage"))
        if wikipedia is not None:
            pageAttrib = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
            pageURL = self.getAttrib(wikipedia, pageAttrib)
            self.logger.debug("Setting wikipedia link {} for entity".format(pageURL))
            entityDict["wikipedia"] = pageURL
        self.metadata["entities"].append(entityDict)

    # This does a similar action with subjects
    def _getSubjects(self):
        subjects = self.getRepeatingField("dcterms", "subject")
        self.metadata["subjects"] = list(map(self._loadSubject, subjects))

    # This parses the RDF subject areas and stores them in our metadata dict
    def _loadSubject(self, subject):
        self.current = subject
        memberTag, member = self.getField(("dcam", "memberOf"))
        memberAttrib = "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource"
        subjectMember = self.getAttrib(member, memberAttrib)
        subjectTag, subjectText = self.getField(("rdf", "value"))
        self.logger.debug("Storing subject {}".format(subjectText.text))
        return {
            "source": subjectMember,
            "subject": subjectText.text
        }

    # This creates a stub editions/instances section of the metadata dict
    # It populates the list with a single edition -- the Gutenberg edition that
    # forms the basis of this record
    def _createEditions(self):
        self.metadata["editions"] = []
        gutenbergEdition = {
            "pubPlace": "",
            "publisher": self.metadata["publisher"],
            "year": self.metadata["issued"][:4],
            "title": self.metadata["title"],
            "extent": None,
            "dimensions": None,
            "notes": None,
            "language": self.metadata["language"],
            "isbn": [],
            "issn": [],
            "oclc": []
        }
        # Having stored this information on the edition, we pop these fields
        # off of the main work record (part of FRBR-ization)
        self.metadata["editions"].append(gutenbergEdition)
        self.metadata.pop("publisher", None)
        self.metadata.pop("issued", None)

    # This is a basic helper function that loads a conversion list for the MARC
    # rel codes grabbed from LC (http://id.loc.gov/vocabulary/relators.html)
    # It gets us the human-readable equivalents
    def _loadLCRels(self):
        rels = {}
        relatorPath = "/../files/lc_relators.csv"
        with open(os.path.dirname(__file__) + relatorPath) as relFile:
            relReader = csv.reader(relFile)
            for row in relReader:
                rels[row[1]] = row[2].lower()
        return rels
