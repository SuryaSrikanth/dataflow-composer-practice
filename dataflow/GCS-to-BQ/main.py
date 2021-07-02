import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
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

        self.data = {
            "Student_name":'',
            "Roll_number":'',
            "registration_number":'',
            "Branch":'',
            "Address1":'',
            "Address2":''
        }

    def parse(self, line):
        user_data = line.split(',')
        self.data['Student_name'] = str(user_data[0]).strip()
        self.data['Roll_number'] = str(user_data[1]).strip()
        self.data['registration_number'] = str(user_data[2]).strip()
        self.data['Branch'] = str(user_data[3]).strip()
        self.data['Address1'] = str(user_data[4]).strip()
        self.data['Address2'] = str(user_data[5]).strip()
        # print('SELF DATA',self.data)
        return self.data


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://gcs-bq-dataflow-practice/dataflow-test1.csv',
        help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_opts = PipelineOptions(pipeline_args)
    pipeline_opts.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_opts) as p:
        obj = Parser()
        input_data = (
            p
            | 'Read from file' >> ReadFromText(known_args.input,skip_header_lines=1)
            | 'Parse Data' >> beam.Map(lambda line: obj.parse(line))
        )

        input_data | 'Write To BQ' >> WriteToBigQuery(
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