import logging
import os
import csv
from itertools import repeat
from lxml import etree

class xmlParser:

    def __init__(self):
        self.logger = logging.getLogger('guten_logs')

        # Necessary for lxml to properly parse namespaced elements and attribs
        self.nsmap = {}

        self.root = None
        self.current = None

    def parse(self, rdfFile):
        rdf = etree.parse(rdfFile)
        self.root = rdf.getroot()

    def parseString(self, xmlString):
        self.root = etree.fromstring(xmlString)

    def loadRecord(self, ns, tag):
        xpath = self._formatXpath(ns, tag)
        self.current = self.root.find(xpath, namespaces=self.nsmap)

    # This loads data from a single field that contains only text
    def getField(self, tag):
        ns, field = tag
        xpath = self._formatXpath(ns, field)
        xmlData = self.current.find(xpath, namespaces=self.nsmap)
        return field, xmlData

    def getFields(self, tags):
        return list(map(self.getField, tags))

    def getRepeatingField(self, ns, field):
        xpath = self._formatXpath(ns, field)
        return self.current.findall(xpath, namespaces=self.nsmap)

    def getAttrib(self, field, attrib):
        return field.get(attrib)

    def loadNamespaces(self, namespaces):
        list(map(self.addNamespace, namespaces))

    def addNamespace(self, ns):
        tag, urn = ns
        self.nsmap[tag] = urn

    def _formatXpath(self, ns, tag):
        if ns is None:
            xpath = ".//{}".format(tag)
        else:
            xpath = ".//{}:{}".format(ns, tag)
        return xpath
