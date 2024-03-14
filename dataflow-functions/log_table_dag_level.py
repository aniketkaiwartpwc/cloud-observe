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
    def process(self,element):
        try:
            dict_line = json.loads(element)
            sub_str = dict_line['textPayload']
            
            st = '{' + "'Run_Timestamp':'" + dict_line['timestamp'] + "','DagName':'" + sub_str.split()[4].split('=')[1].split(',')[0] + "','DagStatus':'" + sub_str.split()[3].split('.')[0] + "','Start_Date':'" + sub_str.split()[7].split('.')[0] + "','End_Date':'" + sub_str.split()[8].split('.')[0] + "'}"
            st = st.replace("'",'"')
#            print("st_output",st)
            dag_status = sub_str.split()[3].split('.')[0]
            status_list = ['FAILED','SUCCESS','UP_FOR_RETRY']
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
                  p | 'Read from GCS' >> ReadFromText(f'gs://dataobs/airflow-worker/{today_date}/*.json')
                    | 'Parse logs to string representation of dict' >> beam.ParDo(parseJSON())
                    | 'Convert String to Dict' >> beam.Map(lambda x: json.loads(x))
        )
        #print("result",result)

        write_to_bq = result | 'Write parsed results to BigQuery' >> beam.io.Write(beam.io.WriteToBigQuery(
                                                                                'airflowlog_test_daglevel',
                                                                                dataset = 'airflow_log_capture',
                                                                                project= 'pg-us-n-app-119329',
                                                                                schema= 'Run_Timestamp:TIMESTAMP, DagName:STRING, DagStatus:STRING, Start_Date:TIMESTAMP, End_Date:TIMESTAMP',
                                                                                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                                            )
                                                                )     


if __name__ == "__main__":
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    run() 

