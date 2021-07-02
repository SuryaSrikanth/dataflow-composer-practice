import argparse
import logging
import json
import apache_beam as beam

from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from google.cloud.pubsub import PublisherClient


class ParserConverter(beam.DoFn):
    def process(self, line):
        data = {
            "Student_name":'',
            "Roll_number":'',
            "registration_number":'',
            "Branch":'',
            "Address1":'',
            "Address2":''
        }
        user_data = line.split(',')
        data['Student_name'] = str(user_data[0]).strip()
        data['Roll_number'] = int(str(user_data[1]).strip())
        data['registration_number'] = str(user_data[2]).strip()
        data['Branch'] = str(user_data[3]).strip()
        data['Address1'] = str(user_data[4]).strip()
        data['Address2'] = str(user_data[5]).strip()
        record =json.dumps(data).encode("utf-8")
        print('DATA IS,', record)

                
        project_id = 'fast-ability-317910'
        topic_id = 'student_information_topic'
        publisher_client = PublisherClient()
        topic_path = publisher_client.topic_path(project_id, topic_id)
        future = publisher_client.publish(topic_path, record)
        return f"Published message ID: {future.result()}"


def publish_msg(data, publisher_client, topic_path):
    future = publisher_client.publish(topic_path, data)
    return f"Published message ID: {future.result()}"


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='gs://gcs-bq-dataflow-practice/dataflow-test1.csv',
        help='Input file to process.')

    parser.add_argument(
        '--topic',
        dest='topic',
        default='projects/fast-ability-317910/topics/student_information_topic',
        help='Pubsub Topic.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_opts = PipelineOptions(pipeline_args)
    pipeline_opts.view_as(SetupOptions).save_main_session = save_main_session
    pipeline_opts.view_as(StandardOptions).streaming = True


    with beam.Pipeline(options=pipeline_opts) as p:

        input_data = (
            p
            | 'Read from file' >> ReadFromText(known_args.input,skip_header_lines=1)            
            | 'Parse Data and publish' >> beam.ParDo(ParserConverter())
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# python main.py --runner DataflowRunner --region asia-south1 --project fast-ability-317910 --staging_location gs://gcs-bq-dataflow-practice/staging --temp_location gs://gcs-bq-dataflow-practice/temp