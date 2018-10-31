from elasticsearch import Elasticsearch
from elasticsearch_dsl import InnerDoc, DocType, Text, Keyword, Nested, Date, Integer, Search
from elasticsearch_dsl.connections import connections
from helpers.postgres import postgresManager
from helpers.config import GutenbergConfig

class ElasticWriter(postgresManager):
    def __init__(self, test=False):
        super(ElasticWriter, self).__init__()

        self.config = GutenbergConfig()
        esConfig = "elasticsearch"
        if test is True:
            esConfig = "elasticsearch_TEST"
        self.esOpts = self.config.getConfigSection(esConfig)

        connections.create_connection(
            hosts=self.esOpts["host"],
            port=self.esOpts["port"]
        )

        self.es = Elasticsearch()

    def convertToFieldTuples(self, fieldDict, fieldList):
        pairs = []
        for field in fieldList:
            pairs.append((field, fieldDict[field]))
        return pairs


class sfrDoc():
    def setFields(self, fieldPairs):
        if type(fieldPairs) is tuple:
            fieldPairs = [fieldPairs]
        for field, value in fieldPairs:
            self.__setattr__(field, value)

    def getField(self, field):
        return self.__getattr__(field)


class Entity(InnerDoc, sfrDoc):
    name = Text(fields={'raw': Keyword()})
    sort_name = Text(fields={'raw': Keyword()})
    viaf = Keyword()
    lcnaf = Keyword()
    wikipedia = Keyword()
    birth = Integer()
    death = Integer()
    aliases = Text(fields={'raw': Keyword()})
    role = Keyword()

class Subject(InnerDoc, sfrDoc):
    authority = Keyword()
    subject = Text(fields={'raw': Keyword()})
    uri = Keyword()

class Identifier(InnerDoc, sfrDoc):
    idType = Keyword()
    identifier = Keyword()

class Item(InnerDoc, sfrDoc):
    url = Keyword()
    epub_path = Keyword()
    source = Keyword()
    access = Integer()
    size = Integer()
    date_modified = Date()
    ids = Nested(Identifier)

class Instance(InnerDoc, sfrDoc):
    title = Text()
    pub_date = Integer()
    pub_place = Text()
    publisher = Text()
    extent = Text()
    summary = Text()
    edition = Text()
    copyright = Text()
    items = Nested(Item)
    ids = Nested(Identifier)

class Work(DocType, sfrDoc):
    title = Text()
    uuid = Keyword()
    rights_stmt = Text()
    date_created = Date()
    date_updated = Date()
    summary = Text()
    language = Keyword()
    entities = Nested(Entity)
    instances = Nested(Instance)
    subjects = Nested(Subject)
    ids = Nested(Identifier)

    def save(self, **kwargs):
        self.meta.id = kwargs["id"]
        return super(Work, self).save(**kwargs)

    class Index():
        name="sfr"
