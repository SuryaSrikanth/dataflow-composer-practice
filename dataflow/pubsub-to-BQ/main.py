import argparse
import logging
import json
import apache_beam as beam

from apache_beam.io import ReadFromPubSub
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import WriteToBigQuery




class Parser:
    def __init__(self):
        self.table_specs = bigquery.TableReference(
            projectId='fast-ability-317910',
            datasetId='dataflowtest',
            tableId='testtable'
        )

        self.schema = 'Student_name:string,Roll_number:string,registration_number:string,Branch:string,Address1:string,Address2:string'

    

    def parse(self, line):
        return json.loads(line.decode('utf-8'))

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/fast-ability-317910/subscriptions/student_information_subscriber')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        obj = Parser()

        data = (
            p
            | 'Read from pub sub' >> ReadFromPubSub(subscription= known_args.inputSubscription)
            | 'Convert Data to BQ readable' >> beam.Map(obj.parse) 
        )
        
        data | 'Write To BQ' >> WriteToBigQuery(
            table=obj.table_specs,
            schema=obj.schema,
            method="STREAMING_INSERTS",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# python main.py --runner DataflowRunner --region asia-south1 --project fast-ability-317910 --staging_location gs://gcs-bq-dataflow-practice/staging --temp_location gs://gcs-bq-dataflow-practice/temp