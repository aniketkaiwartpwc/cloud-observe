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

# setting up apache beam pipeline 
beam_options = PipelineOptions(
    save_main_session=True,
    #runner='DirectRunner',
    runner='DataFlowRunner',
    project="pg-us-n-app-119329",
    temp_location="gs://dataobs/tmp",
    region="asia-south1")  

class parseJSON(beam.DoFn):
    def process(self,element):
        try:
            dict_line = json.loads(element)
            sub_str = dict_line['textPayload']
            
            st = '{' + "'JobRunID':'" + sub_str.split()[5].split('=')[1].split(',')[0] + "','Run_Timestamp':'" + dict_line['timestamp'] + "','DagName':'" + sub_str.split()[4].split('=')[1].split(',')[0] + "','DagStatus':'" + sub_str.split()[3].split('.')[0] + "','Start_Date':'" + sub_str.split()[7].split('.')[0] + "','End_Date':'" + sub_str.split()[8].split('.')[0] + "'}"
            st = st.replace("'",'"')
#            print("st_output",st)
            dag_status = sub_str.split()[3].split('.')[0]
            status_list = ['FAILED','SUCCESS','UP_FOR_RETRY','SKIPPED']
            if dag_status in status_list:
                return st.split('\n')
            else:
                print("derived dag status is not found in input json file data")
        except:
            logging.info('Some error Occured')



#Entry fn to run pipeline
def run():
    with beam.Pipeline(options=beam_options) as p:

        result = (
                  p | 'Read from GCS' >> ReadFromText('gs://dataobs/airflow-worker/2024/01/17/*.json')
                    | 'Parse logs to string representation of dict' >> beam.ParDo(parseJSON())
                    | 'Convert String to Dict' >> beam.Map(lambda x: json.loads(x))
        )
      #  print("result",result)

        write_to_bq = result | 'Write parsed results to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
                                                                                'airflowlog_parsed_data',
                                                                                dataset = 'airflow_log_capture',
                                                                                project= 'pg-us-n-app-119329',
                                                                                schema= 'SCHEMA_AUTODETECT',
                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                                            )
                                                                )     

if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    run() 

