import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        '--input',
        dest='input',
        default='./test.txt',
        help='Input file to process.')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_opts = PipelineOptions(pipeline_args)
    pipeline_opts.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_opts) as p:

        output = (
            p
            | 'Read from file' >> ReadFromText(known_args.input)
            | 'Write to File' >> WriteToText('output.txt')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()


# python main.py