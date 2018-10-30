import os
import psycopg2
import requests
import uuid
import re
import logging
from itertools import repeat
from collections import defaultdict

from helpers.postgres import postgresManager

class GutenbergDB(postgresManager):

    epubStore = "files/epubs/"

    def __init__(self):
        super(GutenbergDB, self).__init__()
        self.logger = logging.getLogger('guten_logs')

        self.getCursor()

    #TODO
    # 1) Add identifiers for instances and items
    # 2) Update records if they've changed (important to handle relationships)
    # 3) Generate checksum for epubs
    # 4) Store epubs in s3
    # 5) Add date_created and date_modified fields to all tables
    def insert_record(self, metadata, epubs):
        self.logger.info("Storing new Gutenberg File")
        workID, newIDs = self._checkIDs("work", metadata["ids"])
        if workID is not None:
            return {"status": "existing", "result": 0, "work": workID}

        workID = self._createWork(metadata, newIDs)

        editionIDs = self._createInstances(metadata["editions"], workID, epubs)

        subjectIDs = self._createSubjects(metadata["subjects"], workID)

        entityIDs = self._createEntities(metadata["entities"], workID)

        self.commitOps()

        return {"status": "new", "result": 0, "work": workID}

    def _checkIDs(self, table, ids):
        idStmt = self.generateInsert("identifiers", ["type", "identifier"])
        newIDs = []
        tableID = None
        for iden in ids:
            existingID = self.getByID(iden["id"], table)
            if existingID is not None:
                tableID = existingID
            else:
                newID = self.insertRow(idStmt, [iden["type"], iden["id"]])
                newIDs.append(newID)
        return tableID, newIDs

    def _relatedIDs(self, table, tableID, ids):
        linkTable = "{}_identifiers".format(table)
        linkID = "{}_id".format(table)
        linkStmt = self.generateInsert(linkTable, [linkID, "identifier_id"])
        newIDs = []
        for iden in ids:
            newID = self.insertRow(linkStmt, [tableID, iden])
            newIDs.append(newID)
        return newIDs

    # This matches entities in the database based off their names, lifespan and
    # control numbers. If a match is found, return the row ID of the entity
    #
    # TODO Also return the relationship number so we don't create duplicate
    # relationships
    def _matchEntity(self, entity):
        scores = defaultdict(int)
        for field in ["viaf", "lcnaf"]:
            if entity[field] is not None:
                entityID = self.checkForRow("entities", {field: entity[field]})
                if entityID is not None:
                    scores[entityID] += 1
        # Use jaro_winkler algorithim and lifespan to get better matches
        entityRec = self.queryJaroWinkler("entities", "name", entity["name"], 0.9)
        if entityRec:
            scores[entityRec["id"]] += 1
            if self._matchLifespan(entity, entityRec) is True:
                scores[entityRec["id"]] += 1
        for ent in sorted(scores, key=scores.get, reverse=True):
            if scores[ent] >= 2:
                return ent
        return None

    def _matchLifespan(self, entity, dbResult):
        # TODO Pass partial score if birth or death dates match?
        # Also handle None values for only one date? (Known to exist)
        try:
            if(
                dbResult["birth"] == int(entity["birthdate"]) and
                dbResult["death"] == int(entity["deathdate"])
            ):
                return True
        except TypeError:
            self.logger.debug("Lifespan dates include null value, skip")
        except KeyError:
            self.logger.debug("Record is missing birth/death dates")
        return False

    def _createWork(self, metadata, newIDs):
        # TODO
        # Generate namespaced UUID
        workRec = self.queryJaroWinkler("works", "title", metadata["title"], 0.9)
        if workRec:
            entityID = None
            entMatches = list(filter(lambda x: x, map(self._matchEntity, metadata["entities"])))
            if len(entMatches) > 0:
                return workRec["id"]

        workFields = ["uuid", "title", "rights_stmt", "language"]
        workValues = [
            uuid.uuid4().hex,
            metadata["title"],
            metadata["rights"],
            metadata["language"]
        ]

        worksStmt = self.generateInsert("works", workFields)
        workID = self.insertRow(worksStmt, workValues)
        linkedIDs = self._relatedIDs("work", workID, newIDs)
        return workID

    def _createInstances(self, editions, workID, epubs):
        # TODO
        # Check if edition exists based off general identifiers
        editionIDs = []
        instanceFields = [
            "title",
            "pub_date",
            "pub_place",
            "publisher",
            "extent", "summary",
            "language",
            "work_id"
        ]
        editionStmt = self.generateInsert("instances", instanceFields)

        editionIDs = list(map(self._createInstance, editions, repeat(editionStmt), repeat(workID), repeat(epubs)))

        return editionIDs

    def _createInstance(self, edition, editionStmt, workID, epubs):
        isbns = self._generateIdDict("isbn", edition["isbn"])
        editionID, newISBNids = self._checkIDs("instance", isbns)
        issns = self._generateIdDict("issn", edition["issn"])
        editionID, newISSNids = self._checkIDs("instance", issns)
        oclcs = self._generateIdDict("oclc", edition["oclc"])
        editionID, newOCLCids = self._checkIDs("instance", oclcs)
        editionValues = [
            edition["title"],
            edition["year"],
            edition["pubPlace"],
            edition["publisher"],
            edition["extent"],
            edition["notes"],
            edition["language"],
            workID
        ]
        editionID = self.insertRow(editionStmt, editionValues)

        newIDs = newISBNids + newISSNids + newOCLCids
        newEditionIDs = self._relatedIDs("instance", editionID, newIDs)

        # Add Gutenberg items
        if (
            edition["publisher"] is not None and
            'gutenberg' in edition["publisher"].lower()
        ):
            itemIDs = self._createItems(editionID, epubs)

        return editionID

    def _generateIdDict(self, idType, ids):
        return [{"type": idType, "id": iden} for iden in ids]

    def _createItems(self, instanceID, items):
        itemIDs = []
        itemFields = [
            "url",
            "epub_path",
            "source",
            "date_modified",
            "size",
            "access_policy",
            "instance_id"
        ]
        itemStmt = self.generateInsert("items", itemFields)
        itemIDs = list(map(self._createItem, items, repeat(itemStmt), repeat(instanceID)))
        return itemIDs

    def _createItem(self, epub, itemStmt, instanceID):
        self.logger.debug(epub)
        itemValues = [
            epub["url"],
            self._retrieveEpub(epub["url"]),
            "gutenberg",
            epub["updated"],
            epub["size"],
            1,
            instanceID
        ]
        itemID = self.insertRow(itemStmt, itemValues)
        return itemID

    def _createSubjects(self, subjects, workID):
        subjectIDs = []
        # TODO Get authority URIs for all subjects
        subjectFields = ["authority", "subject"]
        subjectRelFields = ["work_id", "subject_id", "weight"]
        subjectStmt = self.generateInsert("subjects", subjectFields)
        subjectRelStmt = self.generateInsert("subject_works", subjectRelFields)

        subjectIDs = list(map(self._createSubject, subjects, repeat(subjectStmt), repeat(subjectRelStmt), repeat(workID)))
        return subjectIDs

    def _createSubject(self, subject, subjectStmt, subjectRelStmt, workID):
        subjectID = self.checkForRow("subjects", subject)
        if subjectID is None:
            subjectValues = [
                subject["source"],
                subject["subject"]
            ]
            subjectID = self.insertRow(subjectStmt, subjectValues)
        relValues = [
            workID,
            subjectID,
            1 # This is the weight of the subject, will potentially be dynamic
        ]
        subjectRelID = self.insertRow(subjectRelStmt, relValues)
        return subjectID

    def _createEntities(self, entities, workID):
        entityIDs = []
        entityID = None
        entityFields = [
            "name",
            "sort_name",
            "viaf",
            "lcnaf",
            "birth",
            "death",
            "wikipedia",
            "aliases"
        ]
        entityStmt = self.generateInsert("entities", entityFields)
        entityRelFields = ["work_id", "entity_id", "role"]
        relStmt = self.generateInsert("entity_works", entityRelFields)

        entityIDs = list(map(self._createEntity, entities, repeat(entityFields), repeat(entityStmt), repeat(relStmt), repeat(workID)))

        return entityIDs

    def _createEntity(self, entity, entityFields, entityStmt, relStmt, workID):
        # Test for existing entity, see method for algorithim rules
        entityID = self._matchEntity(entity)
        if entityID is None:
            entity = self._addMissingFields(entity, entityFields)
            sort_name = entity["sort_name"]
            if entity["sort_name"] is None:
                sort_name = entity["name"]
            entityValues = [
                entity["name"],
                sort_name,
                entity["viaf"],
                entity["lcnaf"],
                entity["birthdate"],
                entity["deathdate"],
                entity["wikipedia"],
                entity["alias"]
            ]
            entityID = self.insertRow(entityStmt, entityValues)

            self._createEntityRel(workID, entityID, entity["role"], relStmt)
        else:
            columns, values = self._generateUpdate(entity, "entities", entityID)
            if len(columns) > 0:
                self.updateRow("entities", entityID, columns, values)

        return entityID

    def _createEntityRel(self, workID, entityID, role, relStmt):
        relValues = [
            workID,
            entityID,
            role
        ]
        entityRelID = self.checkForRow("entity_works", {
            "work_id": workID,
            "entity_id": entityID,
            "role": role
        })
        if entityRelID is None:
            entityRelID = self.insertRow(relStmt, relValues)

    def _addMissingFields(self, record, fields):
        for field in fields:
            if field not in record:
                record[field] = None
        return record

    def _generateUpdate(self, new, table, recordID):
        old = self.getRow(table, recordID)
        columns = []
        values = []
        skips = ["role", "gutenberg_id"]
        for key, value in new.items():
            if key in skips:
                continue
            if old[self.keyReplace(key)] is None and new[key] is not None:
                columns.append(self.keyReplace(key))
                values.append(value)
        return columns, values

    # TODO
    # 1) Generate Checksum
    # 2) "Break" open and store resulting directory
    # 3) Store resulting directory in s3
    def _retrieveEpub(self, epub_url):
        epub = requests.get(epub_url)
        tmpFile = None
        epubFile = re.search(r'[0-9]+.epub.(?:|no)images', epub_url).group(0)
        if epub.status_code == 200:
            self.logger.debug("Storing downloaded epub from " + epub_url)
            tmpFile = GutenbergDB.epubStore + epubFile
            opnFile = open(tmpFile, "wb")
            opnFile.write(epub.content)
            opnFile.close()
        else:
            self.logger.debug("Download Error! Status {}".format(epub.status_code))
        return tmpFile
