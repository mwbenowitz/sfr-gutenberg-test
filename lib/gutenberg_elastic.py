from helpers.elasticsearch import ElasticWriter, Work, Instance, Item, Subject, Entity, Identifier

class GutenbergES():

    esWriter = ElasticWriter()

    getWork = "SELECT * FROM works WHERE id={};"
    getInstances = "SELECT * FROM instances WHERE work_id={}"

    getItems = "SELECT * FROM items WHERE instance_id={}"

    getSubjects = """
        SELECT * FROM subjects s
        JOIN subject_works sw ON s.id = sw.subject_id
        JOIN works w ON w.id = sw.work_id
        WHERE w.id={}
    """

    getEntities = """
        SELECT * FROM entities e
        JOIN entity_works ew ON e.id = ew.entity_id
        JOIN works w ON w.id = ew.work_id
        WHERE w.id={}
    """

    getWorkIDs = """
        SELECT * FROM identifiers i
        JOIN work_identifiers il ON il.identifier_id = i.id
        JOIN works t ON t.id = il.work_id
        WHERE t.id = {}
    """

    getInstanceIDs = """
        SELECT * FROM identifiers i
        JOIN instance_identifiers il ON il.identifier_id = i.id
        JOIN instances t ON t.id = il.instance_id
        WHERE t.id = {}
    """

    workFields = ["title", "uuid", "rights_stmt", "date_created", "date_updated", "language"]
    instanceFields = ["title", "pub_date", "pub_place", "publisher", "language"]
    itemFields = ["url", "epub_path", "source", "size", "date_modified"]
    subjectFields = ["authority", "subject"]
    entityFields = ["name", "sort_name", "aliases", "viaf", "lcnaf", "wikipedia", "birth", "death", "role"]
    identifierFields = ["type", "identifier"]

    def __init__(self):
        self.workID = None

    def storeES(self, workID):
        work = GutenbergES.esWriter.execSelectOne(GutenbergES.getWork, workID)

        fieldPairs = GutenbergES.esWriter.convertToFieldTuples(work, GutenbergES.workFields)
        esWork = Work()
        esWork.setFields(fieldPairs)

        workID = work["id"]
        instances = GutenbergES.esWriter.execSelect(GutenbergES.getInstances, workID)
        for instance in instances:
            instancePairs = GutenbergES.esWriter.convertToFieldTuples(instance, GutenbergES.instanceFields)
            esInstance = Instance()
            esInstance.setFields(instancePairs)
            instanceID = instance["id"]
            items = GutenbergES.esWriter.execSelect(GutenbergES.getItems, instanceID)
            for item in items:
                itemPairs = GutenbergES.esWriter.convertToFieldTuples(item, GutenbergES.itemFields)
                esItem = Item()
                esItem.setFields(itemPairs)
                esInstance.items.append(esItem)

            identifiers = GutenbergES.esWriter.execSelect(GutenbergES.getInstanceIDs, instanceID)
            for iden in identifiers:
                idenPairs = GutenbergES.esWriter.convertToFieldTuples(iden, GutenbergES.identifierFields)
                esIden = Identifier()
                esIden.setFields(idenPairs)
                esInstance.ids.append(esIden)

            esWork.instances.append(esInstance)

        entities = GutenbergES.esWriter.execSelect(GutenbergES.getEntities, workID)
        for entity in entities:
            entityPairs = GutenbergES.esWriter.convertToFieldTuples(entity, GutenbergES.entityFields)
            esEntity = Entity()
            esEntity.setFields(entityPairs)
            esWork.entities.append(esEntity)

        subjects = GutenbergES.esWriter.execSelect(GutenbergES.getSubjects, workID)
        for subject in subjects:
            subjectPairs = GutenbergES.esWriter.convertToFieldTuples(subject, GutenbergES.subjectFields)
            esSubject = Subject()
            esSubject.setFields(subjectPairs)
            esWork.subjects.append(esSubject)

        identifiers = GutenbergES.esWriter.execSelect(GutenbergES.getWorkIDs, workID)
        for iden in identifiers:
            idenPairs = GutenbergES.esWriter.convertToFieldTuples(iden, GutenbergES.identifierFields)
            esIden = Identifier()
            esIden.setFields(idenPairs)
            esWork.ids.append(esIden)

        esWork.save()
