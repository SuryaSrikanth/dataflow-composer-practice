import argparse
import logging
import json
import apache_beam as beam
import datetime
import random
from apache_beam.io import ReadFromPubSub, ReadFromText
from apache_beam.transforms.window import FixedWindows
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import WriteToBigQuery
from apache_beam import pvalue
from apache_beam import DoFn, GroupByKey, io, ParDo, Pipeline, PTransform, WindowInto, WithKeys


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )
   

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


    def parse_pubsub(self, line):
        return json.loads(line.decode('utf-8'))

    def is_present(self, pubsub_data, gcs_data):
        if (pubsub_data in gcs_data):
            return True
        else:
            return False



def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument('--input',
                        dest='input',
                        default='gs://gcs-bq-dataflow-practice/dataflow-test1.csv',
                        help='Input file to process.')
    
    parser.add_argument('--inputSubscription',
                        dest='inputSubscription',
                        help='input subscription name',
                        default='projects/fast-ability-317910/subscriptions/student_information_subscriber')

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=pipeline_options) as p:
        obj = Parser()

        side_input = (
            p
            | 'Read from GCS' >> ReadFromText(known_args.input,skip_header_lines=1)
            | 'Convert Data to Dictionary' >> beam.Map(obj.parse)
        )

        pubsub_data = (
            p
            | 'Read from pub sub' >> ReadFromPubSub(subscription= known_args.inputSubscription)
            | 'Window' >> beam.WindowInto(beam.window.SlidingWindows(2,1))
            | 'Parse pubsub to Dictionary' >> beam.Map(obj.parse_pubsub)
            | 'Filter with side input' >> beam.Map(obj.is_present, pvalue.AsIter(side_input))
            | 'Write to BQ' >> WriteToBigQuery(
            table=obj.table_specs,
            schema=obj.schema,
            method="STREAMING_INSERTS",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

# python main.py --runner DataflowRunner --region asia-south1 --project fast-ability-317910 --staging_location gs://gcs-bq-dataflow-practice/staging --temp_location gs://gcs-bq-dataflow-practice/temp