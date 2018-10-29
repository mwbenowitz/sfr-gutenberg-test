import os
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import uuid
import re
import logging
from itertools import repeat
from collections import defaultdict

from helpers.args import GutenbergArgs
from helpers.config import GutenbergConfig

class postgresManager:

    replaceKeys = {
        "id": "identifier",
        "source": "authority",
        "birthdate": "birth",
        "deathdate": "death",
        "alias": "aliases"
    }

    config = GutenbergConfig()
    postgresOpts = config.getConfigSection("postgresql")

    def __init__(self):
        self.logger = logging.getLogger('guten_logs')
        self.conn = psycopg2.connect("dbname={} user={} password={}".format(
            postgresManager.postgresOpts["db"],
            postgresManager.postgresOpts["user"],
            postgresManager.postgresOpts["password"]
        ))
        self._checkDB()

    def getCursor(self):
        self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)

    def commitOps(self):
        self.conn.commit()

    def closeCursor(self):
        self.cursor.close()

    def closeAll(self):
        self.closeCursor()
        self.conn.close()

    def execSelect(self, selectStmt, iden=None):
        self.cursor.execute(selectStmt.format(iden))
        return self.cursor.fetchall()

    def execSelectOne(self, selectStmt, iden=None):
        self.cursor.execute(selectStmt.format(iden))
        return self.cursor.fetchone()

    def getRow(self, table, rowID):
        getQuery = "SELECT * FROM {} WHERE id={}".format(table, rowID)
        self.cursor.execute(getQuery)
        return self.cursor.fetchone()

    def generateInsert(self, table, columns):
        placeholders = ", ".join(["%s"] * len(columns))
        columnNames = [col.upper() for col in columns]
        return """
            INSERT INTO {} ({}) VALUES ({}) RETURNING id
        """.format(table, ', '.join(columnNames), placeholders)

    # All insert statements should return the id of the newly inserted row
    def insertRow(self, query, values):
        self.cursor.execute(query, values)
        return self.cursor.fetchone()["id"]

    def updateRow(self, table, rowID, columns, values):
        setStmts = []
        for i, col in enumerate(columns):
            setStmts.append("{}='{}'".format(col, values[i]))
        setStmt = ", ".join(setStmts)

        updateQuery = """
            UPDATE {} SET {} WHERE id={}
        """.format(table, setStmt, rowID)

        self.cursor.execute(updateQuery)


    def checkForRow(self, table, values):
        whereStmts = []
        for key, value in values.items():
            escapedValue = self._escapeQuotes(value)
            whereQuery = "{}='{}'".format(self.keyReplace(key), escapedValue)
            whereStmts.append(whereQuery)
        query = """
            SELECT id FROM {} WHERE {}
        """.format(table, " AND ".join(whereStmts))
        self.cursor.execute(query)
        if self.cursor.rowcount > 0:
            return self.cursor.fetchone()["id"]
        return None

    def queryJaroWinkler(self, table, field, value, score):
        escapedValue = self._escapeQuotes(value)
        query = """
            SELECT * FROM {} WHERE jarowinkler({}, '{}') > {}
        """.format(table, field, escapedValue, score)
        self.cursor.execute(query)
        if self.cursor.rowcount > 0:
            return self.cursor.fetchone()
        return None

    def getByID(self, id, table):
        query = """
            SELECT t.id FROM {}s AS t
            JOIN {}_identifiers AS l
            ON t.id = l.{}_id
            JOIN identifiers AS i
            ON l.identifier_id = i.id
            WHERE i.identifier='{}'
        """.format(table, table, table, id)
        self.cursor.execute(query)
        if self.cursor.rowcount > 0:
            return self.cursor.fetchone()["id"]
        return None

    def getRecordsByID(self, id):
        existing = defaultdict(set)
        for table in ["work", "instance", "item"]:
            id = self.getByID(id, table)
            existing[table].add(id)
        return existing

    def _escapeQuotes(self, value):
        try:
            escaped = value.replace("'", "''")
            return escaped
        except AttributeError:
            return value

    def keyReplace(self, key):
        if key in postgresManager.replaceKeys:
            return postgresManager.replaceKeys[key]
        return key

    def _checkDB(self):
        self.getCursor()
        self.cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
        """)
        if len(self.cursor.fetchall()) < 1:
            self.logger.info("Database does not exist!, Creating")

            self.cursor.execute("""
                CREATE TABLE works (
                    id              SERIAL PRIMARY KEY,
                    uuid            UUID NOT NULL,
                    title           VARCHAR(255) NOT NULL,
                    rights_stmt     TEXT NULL,
                    rights_url      VARCHAR(125) NULL,
                    rights_source   VARCHAR(125) NULL,
                    rights_access   VARCHAR(25) NULL,
                    date_created    TIMESTAMP NOT NULL DEFAULT NOW(),
                    date_updated    TIMESTAMP NOT NULL DEFAULT NOW(),
                    geo_coverage    VARCHAR(255) NULL,
                    temp_coverage   VARCHAR(255) NULL,
                    summary         TEXT NULL,
                    language        CHAR(2) NOT NULL DEFAULT 'en'
                )
            """)

            self.cursor.execute("""
                CREATE TABLE instances (
                    id              SERIAL PRIMARY KEY,
                    title           TEXT NOT NULL,
                    pub_date        INT NULL,
                    pub_place       VARCHAR(255) NULL,
                    publisher       VARCHAR(255) NULL,
                    extent          VARCHAR(255) NULL,
                    edition         VARCHAR(125) NULL,
                    edition_stmt    VARCHAR(125) NULL,
                    summary         TEXT NULL,
                    issuance        VARCHAR(255) NULL,
                    copyright_date  DATE NULL,
                    toc             TEXT NULL,
                    language        CHAR(2) NULL,
                    work_id         INT REFERENCES works(id)
                )
            """)

            self.cursor.execute("""
                CREATE TABLE items (
                    id              SERIAL PRIMARY KEY,
                    url             VARCHAR(255) NOT NULL,
                    epub_path       VARCHAR(255) NULL,
                    source          VARCHAR(255) NULL,
                    date_modified   DATE NULL,
                    size            INT NULL,
                    checksum        CHAR(32) NULL,
                    access_policy   SMALLINT NOT NULL,
                    instance_id     INT REFERENCES instances(id)
                )
            """)

            self.cursor.execute("""
                CREATE TABLE entities (
                    id              SERIAL PRIMARY KEY,
                    name            VARCHAR(255) NOT NULL,
                    sort_name       VARCHAR(255) NOT NULL,
                    viaf            VARCHAR(125) NULL,
                    lcnaf           VARCHAR(125) NULL,
                    birth           SMALLINT NULL,
                    death           SMALLINT NULL,
                    wikipedia       VARCHAR(125) NULL,
                    aliases         VARCHAR(255) NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE entity_works (
                    id              SERIAL PRIMARY KEY,
                    work_id         INT REFERENCES works(id),
                    entity_id       INT REFERENCES entities(id),
                    role            VARCHAR(50) NOT NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE entity_instances (
                    id              SERIAL PRIMARY KEY,
                    instance_id     INT REFERENCES instances(id),
                    entity_id       INT REFERENCES entities(id),
                    role            VARCHAR(50) NOT NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE entity_items (
                    id              SERIAL PRIMARY KEY,
                    item_id         INT REFERENCES items(id),
                    entity_id       INT REFERENCES entities(id),
                    role            VARCHAR(50) NOT NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE subjects (
                    id              SERIAL PRIMARY KEY,
                    authority       VARCHAR(125) NOT NULL,
                    uri             VARCHAR(255) NULL,
                    subject         TEXT NOT NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE subject_works (
                    id              SERIAL PRIMARY KEY,
                    work_id         INT REFERENCES works(id),
                    subject_id      INT REFERENCES subjects(id),
                    weight          FLOAT(5) NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE identifiers (
                    id              SERIAL PRIMARY KEY,
                    type            VARCHAR(125) NOT NULL,
                    identifier      VARCHAR(255) NOT NULL
                )
            """)

            self.cursor.execute("""
                CREATE TABLE work_identifiers (
                    id              SERIAL PRIMARY KEY,
                    work_id         INT REFERENCES works(id),
                    identifier_id   INT REFERENCES identifiers(id)
                )
            """)

            self.cursor.execute("""
                CREATE TABLE instance_identifiers (
                    id              SERIAL PRIMARY KEY,
                    instance_id     INT REFERENCES instances(id),
                    identifier_id   INT REFERENCES identifiers(id)
                )
            """)

            self.cursor.execute("""
                CREATE TABLE item_identifiers (
                    id              SERIAL PRIMARY KEY,
                    item_id         INT REFERENCES items(id),
                    identifier_id   INT REFERENCES identifiers(id)
                )
            """)
            self.commitOps()
            self.closeCursor()
