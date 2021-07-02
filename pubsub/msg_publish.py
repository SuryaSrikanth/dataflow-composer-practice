# from avro.io import DatumWriter
# import avro
# import io
import json

# from google.cloud import pubsub_v1
# from google.api_core.exceptions import NotFound
from google.cloud.pubsub import PublisherClient
project_id = 'fast-ability-317910'
topic_id = 'student_information_topic'
publisher_client = PublisherClient()
topic_path = publisher_client.topic_path(project_id, topic_id)

# avro_schema = {
#   "type": "record",
#   "name": "Avro",
#   "fields": [
#     {
#       "name": "Student_name",
#       "type": "string"
#     },
#     {
#       "name": "Roll_number",
#       "type": "long"
#     },
#     {
#       "name": "registration_number",
#       "type": "string"
#     },
#     {
#       "name": "Branch",
#       "type": "string"
#     },
#     {
#       "name": "Address1",
#       "type": "string"
#     },
#     {
#       "name": "Address2",
#       "type": "string"
#     }
#   ]
# }

# avro_schema = avro.schema.parse(json.dumps(avro_schema))
# writer = DatumWriter(avro_schema)
# bout = io.BytesIO()


record = {
    "Student_name":"PubsubStudentDATAFLOWtest",
    "Roll_number":9876543210,
    "registration_number":"PUBSUB",
    "Branch":"ECE",
    "Address1":"Gandhi Road",
    "Address2":"India"
}

record_present = {
    "Student_name":"A1",
    "Roll_number":3216549873,
    "registration_number":"A",
    "Branch":"CSE",
    "Address1":"house",
    "Address2":"area"
}

data = json.dumps(record).encode("utf-8")
print(f"Preparing a JSON-encoded message:\n{data}")

# publish many times
for _ in range(10):
    future = publisher_client.publish(topic_path, data)
    print(f"Published message ID: {future.result()}")

