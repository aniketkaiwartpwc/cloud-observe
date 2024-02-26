import argparse
import logging
import apache_beam as beam
import re
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.transforms.sql import SqlTransform
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions
import json
import ast
from datetime import datetime

today_date = datetime.strftime(datetime.today(),'%Y/%m/%d')

# setting up apache beam pipeline 
beam_options = PipelineOptions(
    save_main_session=True,
    #runner='DirectRunner',
    runner='DataFlowRunner',
    project="pg-us-n-app-119329",
    temp_location="gs://dataobs/tmp",
    region="asia-south1")  

class parseJSON(beam.DoFn):
    def process(self, element):
        try:
            dict_line = json.loads(element)
            sub_str = dict_line['textPayload']
            task_id = dict_line['labels']['task-id']  # Extract the task-id
            dag_id = dict_line['labels']['workflow']
            timestamp = dict_line['labels']['execution-date']
            if sub_str.startswith('Record'):
                record_count = sub_str.split()[1].replace('[','').replace(']','')
                st = json.dumps({
                    "JobRunID": task_id,
                    "DagName": dag_id,
                    "Run_Timestamp": timestamp,
                    "RecordCount": record_count
                })
                return st.split('\n')
            else:
                logging.info("Derived dag status is not found in input json file data")
        except Exception as e:
            logging.info(f'Some error occurred: {str(e)}')


            



#Entry fn to run pipeline
def run():
    with beam.Pipeline(options=beam_options) as p:

        result = (
                  p | 'Read from GCS' >> ReadFromText(f'gs://dataobs/airflow-worker/{today_date}/*.json')
                    | 'Parse logs to string representation of dict' >> beam.ParDo(parseJSON())
                    | 'Convert String to Dict' >> beam.Map(lambda x: json.loads(x))
        )



        write_to_bq = (
            result 
            | 'Write parsed results to BigQuery' >> beam.io.WriteToBigQuery(
                'airflowlog_record_count',
                dataset = 'airflow_log_capture',
                project= 'pg-us-n-app-119329',
                schema= 'JobRunID:STRING, DagName:STRING, Run_Timestamp:STRING, RecordCount:STRING',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        ) 


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    run() 
