from helpers.elasticsearch import ElasticWriter, Work, Instance, Item, Subject, Entity, Identifier

class GutenbergES():

    esWriter = ElasticWriter()

    getWork = "SELECT * FROM works WHERE work_id={};"
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

    workFields = ["title", "uuid", "rights_stmt", "date_created", "date_updated", "language"]
    instanceFields = ["title", "pub_date", "pub_place", "publisher", "language"]
    itemFields = ["url", "epub_path", "source", "size", "date_modified"]
    subjectFields = ["authority", "subject"]
    entityFields = ["name", "sort_name", "aliases", "viaf", "lcnaf", "wikipedia", "birth", "death", "role"]

    def __init__(self):
        self.workID = None

    def storeES(self, workID):
        work = esWriter.execSelect(getWork, workID)

        fieldPairs = esWriter.convertToFieldTuples(work, workFields)
        esWork = Work()
        esWork.setFields(fieldPairs)

        workID = work["id"]
        instances = esWriter.execSelect(getInstances, workID)
        for instance in instances:
            instancePairs = esWriter.convertToFieldTuples(instance, instanceFields)
            esInstance = Instance()
            esInstance.setFields(instancePairs)
            instanceID = instance["id"]
            items = esWriter.execSelect(getItems, instanceID)
            for item in items:
                itemPairs = esWriter.convertToFieldTuples(item, itemFields)
                esItem = Item()
                esItem.setFields(itemPairs)
                esInstance.items.append(esItem)

            esWork.instances.append(esInstance)

        entities = esWriter.execSelect(getEntities, workID)
        for entity in entities:
            entityPairs = esWriter.convertToFieldTuples(entity, entityFields)
            esEntity = Entity()
            esEntity.setFields(entityPairs)
            esWork.entities.append(esEntity)

        subjects = esWriter.execSelect(getSubjects, workID)
        for subject in subjects:
            subjectPairs = esWriter.convertToFieldTuples(subject, subjectFields)
            esSubject = Subject()
            esSubject.setFields(subjectPairs)
            esWork.subjects.append(esSubject)

        esWork.save()
