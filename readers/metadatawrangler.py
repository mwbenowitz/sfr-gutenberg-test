import requests
import logging
import re
from lxml import etree

from helpers.xml import xmlParser

class MetadataWranglerReader(xmlParser):

    mwURL = "https://metadata.librarysimplified.org/lookup?urn=http://www.gutenberg.org/ebooks/"

    def __init__(self):
        super(MetadataWranglerReader, self).__init__()
        self.logger = logging.getLogger('guten_logs')

        self.loadNamespaces([
            ("simplified", "http://librarysimplified.org/terms/"),
            ("schema", "http://schema.org/"),
            ("dcterms", "http://purl.org/dc/terms/"),
            (None, "http://www.w3.org/2005/Atom")
        ])

    def getMWData(self, metadata, gutenbergID):
        mwBookURL = "{}{}".format(MetadataWranglerReader.mwURL, gutenbergID)
        mwData = requests.get(mwBookURL)
        if mwData.status_code == 200:
            self.parseString(mwData.content)
            self.loadRecord(None, "entry")
            if self.current is None:
                self.logger.info("No MW metadata avaiable for {}".format(gutenbergID))
                return metadata

            # Get additional author data
            metadata = self._parseAuthor(metadata)
            self.loadRecord(None, "entry")
            metadata = self._loadIDs(metadata)

            # Add language
            langTag, language = self.getField(("dcterms", "language"))
            if language is not None:
                metadata["language"] = language.text
            else:
                metadata["language"] = "en"

        return metadata

    def _parseAuthor(self, metadata):
        authorFields = {
            "sort_name": None,
            "viaf": None,
            "lcnaf": None,
        }
        authorTag, author = self.getField((None, "author"))
        if author is None:
            return metadata

        self.current = author

        sortTag, sort = self.getField(("simplified", "sort_name"))
        if sort is not None:
            sort_name = sort.text.strip("., ")
            authorFields["sort_name"] = sort_name

        nameTag, name = self.getField((None, "name"))
        if name.text is not None:
            nameText = name.text.strip("., ")
        else:
            nameText = sort_name

        authorIDs = self.getRepeatingField("schema", "sameas")
        for authorID in authorIDs:
            idString = authorID.text
            if 'viaf' in idString:
                authorFields["viaf"] = self._getControlNumber(idString)
            elif 'authorities/names' in idString:
                authorFields["lcnaf"] = self._getControlNumber(idString)

        for i, entity in enumerate(metadata["entities"]):
            if entity["name"] in [sort_name, name]:
                enhancedEntity = {**entity, **authorFields}
                metadata["entities"][i] = enhancedEntity

        return metadata

    def _loadIDs(self, metadata):
        ids = self.getRepeatingField(None, "id")
        metadata["ids"] = []
        for iden in ids:
            if len([x for x in metadata["ids"] if x["id"] == iden]) > 0:
                continue
            metadata["ids"].append({
                "type": "gutenberg",
                "id": iden.text
            })
        return metadata

    def _getControlNumber(self, url):
        ctrlNo = re.search(r"([a-z]*[0-9]+)$", url)
        if ctrlNo is not None:
            return ctrlNo.group(0)
        return None
