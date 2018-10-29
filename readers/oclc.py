import logging
import requests
import marcalyx
import re
import sys
from Levenshtein import distance, jaro_winkler
from lxml import etree

from helpers.config import GutenbergConfig
from helpers.xml import xmlParser

class oclcReader(xmlParser):

    config = GutenbergConfig()
    wsKey = config.getConfigValue("api_keys", "wskey")

    oclcSearch = "http://www.worldcat.org/webservices/catalog/search/worldcat/sru?query="
    oclcClassify = "http://classify.oclc.org/classify2/Classify?oclc="
    oclcCatalog = "http://www.worldcat.org/webservices/catalog/content/"

    def __init__(self):
        super(oclcReader, self).__init__()
        self.logger = logging.getLogger('guten_logs')

        self.loadNamespaces([
            (None, "http://www.loc.gov/MARC21/slim")
        ])

    def getOCLCData(self, metadata):
        query = self._createQuery(metadata)
        self.logger.debug("Search Query: {}".format(query))
        oclcResp = requests.get(query)
        # TODO Handle error codes in a real way here
        oclcResp.raise_for_status

        # Load returned MARC records
        oclc = self._getGutenbergOCLC(oclcResp)
        self.logger.debug(oclc)
        if oclc is False:
            return False
        workID, workTitle, editionOCLCs, workAuthors = self._getEditions(oclc)
        self.logger.debug("Loaded OCLC records for OWI {}".format(workID))
        if workID is None and editionOCLCs is None:
            return False
        self.logger.debug("Got Edition data from OCLC")
        editions = self._getEditionMARC(editionOCLCs)
        metadata["entities"].extend(workAuthors)
        self._enhanceRecord(metadata, workID, workTitle, editions)

    def _enhanceRecord(self, metadata, workID, workTitle, marcEditions):
        # Add Work level data
        # TODO Handle Variant Titles, right now each edition has their own
        metadata["title"] = workTitle
        metadata["ids"].append({
            "type": "owi",
            "id": workID
        })
        editions = metadata["editions"]
        tmpEditions = []
        for edition, language in marcEditions:
            oclc = edition["001"][0].value
            title = self._getSubfield(edition, "245", "a")
            subTitle = self._getSubfield(edition, "245", "b")
            if subTitle is not None:
                title = "{} {}".format(title, subTitle)
            publisher = self._getSubfield(edition, "260", "b")
            pubPlace = self._getSubfield(edition, "260", "a")
            pubYear = re.search(r"([0-9]{4})", self._getSubfield(edition, "260", "c")).group(0)
            extent = self._getSubfield(edition, "300", "a")
            dimensions = self._getSubfield(edition, "300", "c")

            notes = self._getSubfield(edition, "500", "a")

            isbns = edition.isbns()
            issns = edition.issns()
            newEdition = {
                "title": title,
                "publisher": publisher,
                "pubPlace": pubPlace,
                "year": pubYear,
                "extent": extent,
                "dimensions": dimensions,
                "language": language[:2],
                "notes": notes,
                "isbn": isbns,
                "issn": issns,
                "oclc": [oclc]
            }
            editions = self._mergeEdition(editions, newEdition)
        metadata["editions"] = editions
        return metadata


    # This is the main edition matching algorithm. It looks at three fields:
    # 1) Place 2) Publisher 3) year
    # If two of three match then we assert that the editions are the same
    # Matches are calculated off a jaro_winkler comparison based off the following
    # fields
    # 1) Place: > 0.9, 2) Publisher: > 0.9, 3) year: exact
    # jaro_winkler generates a distance scored weighted towards strings that
    # share prefixes. This is useful in this use case as we probably want to
    # match records for publishers like "Penguin" and "Penguin Classics" or
    # "New York City" and "New York" which would fail a Levenshtein check
    # TODO Should this include title?
    def _mergeEdition(self, existing, new):
        for i, edition in enumerate(existing):
            score = 0
            if edition["pubPlace"] is not None and new["pubPlace"] is not None:
                placeDist = jaro_winkler(edition["pubPlace"].lower(), new["pubPlace"].lower())
                if placeDist > 0.9:
                    score += 1
            if edition["publisher"] is not None and new["publisher"] is not None:
                pubDist = jaro_winkler(edition["publisher"].lower(), new["publisher"].lower())
                if pubDist > 0.9:
                    score += 1
            if edition["year"] == new["year"]:
                score += 1
            if score > 1:
                self.logger.debug("Found matching editions")
                existing[i] = self._mergeRecords(edition, new)
                return existing

        self.logger.debug("Found new edition")
        existing.append(new)
        return existing

    # TODO Figure out a way to best merge all fields to preserve as much data
    # as humanly possible. We want these records to be rich to enable discovery
    def _mergeRecords(self, edition, new):
        edition["isbn"] = list(set(edition["isbn"] + new["isbn"]))
        edition["issn"] = list(set(edition["issn"] + new["issn"]))
        edition["oclc"].extend(new["oclc"])
        edition["notes"] = new["notes"]
        return edition


    def _createQuery(self, metadata):
        query = ""
        if metadata["marc010"] is not None:
            query = "srw.dn+all+'{}''".format(metadata["marc010"])
        else:
            query = 'srw.ti+all+"{}"'.format(metadata["title"])
            authors = self._getAuthors(metadata["entities"])
            if len(authors) > 0:
                query += '+and+srw.au+all+"{}"'.format(authors)
        query += "&wskey={}".format(oclcReader.wsKey)
        return oclcReader.oclcSearch + query

    def _getAuthors(self, entities):
        authors = [x["name"] for x in entities if x["role"] == "creator"]
        return ", ".join(authors)

    def _getGutenbergOCLC(self, oclcResp):
        results = etree.fromstring(oclcResp.text.encode("utf-8"))
        records = results.findall(".//record", namespaces=self.nsmap)
        gutenbergMARC = list(filter(lambda x: x, map(self._findGutenbergOCLC, records)))
        if len(gutenbergMARC) > 0:
            return gutenbergMARC[0]
        return False

    def _findGutenbergOCLC(self, record):
        marc = marcalyx.Record(record)
        gutenbergRefs = [holding for holding in marc.holdings() if 'gutenberg' in str(holding.subfield("u")[0])]
        # TODO Does it ever make sense to have multiple records with gutenberg references?
        # If we have more than one, how do we determine which is the correct one
        if len(gutenbergRefs) > 0:
            return marc["001"][0].value
        return False

    def _getEditions(self, oclc):
        classifyQuery = "{}&wskey={}".format(oclc, oclcReader.wsKey)
        classifyQuery = oclcReader.oclcClassify + classifyQuery
        self.logger.debug("Classify Query: {}".format(classifyQuery))

        classifyResp = requests.get(classifyQuery)
        classifyResp.raise_for_status

        self.nsmap[None] = "http://classify.oclc.org"
        classifyXML = etree.fromstring(classifyResp.text.encode("utf-8"))
        classifyCode = classifyXML.find(".//response", namespaces=self.nsmap).attrib["code"]
        if classifyCode == "102":
            return None, None, None, None
        workID = classifyXML.find(".//work", namespaces=self.nsmap).attrib["owi"]
        oclcTitle = classifyXML.find(".//work", namespaces=self.nsmap).attrib["title"]
        editions = classifyXML.findall(".//edition", namespaces=self.nsmap)
        editionOCLCs = [(edition.attrib["oclc"], edition.attrib["language"]) for edition in editions]

        authorRecs = classifyXML.findall(".//author", namespaces=self.nsmap)
        authors = list(map(self._parseAuthors, authorRecs))
        return workID, oclcTitle, editionOCLCs, authors

    def _parseAuthors(self, author):
        return {
            "name": author.text,
            "viaf": author.get("viaf"),
            "lcnaf": author.get("lc"),
            "role": "author",
            "birthdate": None,
            "deathdate": None
        }

    def _getEditionMARC(self, oclcs):
        return list(map(self._loadEdition, oclcs))

    def _loadEdition(self, oclcData):
        oclc, language = oclcData
        catalogQuery = "{}?wskey={}".format(oclc, oclcReader.wsKey)
        catalogQuery = oclcReader.oclcCatalog + catalogQuery
        self.logger.debug("Catalog Query: {}".format(catalogQuery))
        catalogResp = requests.get(catalogQuery)
        catalogResp.raise_for_status

        self.nsmap[None] = "http://classify.oclc.org"
        catalogXML = etree.fromstring(catalogResp.text.encode("utf-8"))
        return (marcalyx.Record(catalogXML), language)

    def _getSubfield(self, marc, field, code):
        if len(marc.subfield(field, code)) < 1:
            return None
        return marc.subfield(field, code)[0].value.strip("[].,;:() ")
