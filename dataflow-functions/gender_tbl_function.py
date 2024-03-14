import apache_beam as beam
from apache_beam.pipeline import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
import logging
import argparse
import re

table_spec = 'dataobs.gender_table'
table_schema='customerID:INT64, Weight:FLOAT, Height:FLOAT, Age:INT64, Gender:STRING, Job_Run_Date:TIMESTAMP'

class DataIngestion:
    def parse_method(self, string_input):
        values = re.split(",", re.sub('\r\n', '', re.sub('"', '',string_input)))
        row = dict(zip(('customerID', 'Weight', 'Height', 'Age', 'Gender', 'Job_Run_Date'),values))
        return row

def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument('--inputfile',dest='input', help='Input file to process.' ,required=True)
    parser.add_argument('--outputfile',dest='output', help='Input file to process.')

    known_args, pipline_args = parser.parse_known_args(argv)
    pipelineoptions = PipelineOptions(pipline_args)
    pipelineoptions.view_as(SetupOptions).save_main_session = save_main_session

    dataingestion = DataIngestion()

    with beam.Pipeline(options=pipelineoptions) as p:

        maindata=(
            p| "Reading file" >> beam.io.ReadFromText(known_args.input,skip_header_lines=1)
            |"Split" >> beam.Map(lambda x:x.split(","))
            | "Data Ingestion " >> beam.Map(lambda s: dataingestion.parse_method(s)) \
            | "Write to BQ" >> beam.io.WriteToBigQuery(table_spec, schema=table_schema,
                                                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                                                       create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
            )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run() 
