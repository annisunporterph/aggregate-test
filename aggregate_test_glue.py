#glue job script to compare the source count vs. target count of the act created in aggregate/ folder
#test

import sys
import boto3
import json
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ph_environment', 'log_level', 'region'])
environment = args['ph_environment']
log_level = args['log_level']
region = args['region']
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#start session here
session3 = boto3.Session()
s3_client = boto3.client('s3')
s3 = boto3.resource('s3')

bucket = s3.Bucket('ph-datasci-dev-data-lake-20190308174735395300000001')

for obj in bucket.objects.filter(Prefix='aggregate/'):
    #print(obj.key)
    content_object= s3.Object(bucket.name, obj.key)

    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)

    startDate = json_content['startDate']
    endDate = json_content['endDate']

    #count of act(from source)
    act_count= json_content['act'] #if there's another field need to be checked, then replace 'act' with eg.'act'
    print(obj.key)
    print("act_count: ", act_count)
    
    ###############
    #athena from s3
    def get_var_char_values(d):
       return [obj['VarCharValue'] for obj in d['Data']]


    def query_results(session, params, wait = True):   
        session = boto3.Session() 
        athena = session.client('athena')
        
        ## This function executes the query and returns the query execution ID
        response_query_execution_id = athena.start_query_execution(
            QueryString = params['query'],
            QueryExecutionContext = {
                'Database' : "ph_datasci-dev_db_data_lake"
            },
            ResultConfiguration = {
                'OutputLocation': 's3://' + params['bucket'] + '/' + params['path']
            }
        )

        if not wait:
            return response_query_execution_id['QueryExecutionId']
        else:
            response_get_query_details = athena.get_query_execution(
                QueryExecutionId = response_query_execution_id['QueryExecutionId']
            )
            status = 'RUNNING'
            iterations = 360 # 30 mins

            while (iterations > 0):
                iterations = iterations - 1
                response_get_query_details = athena.get_query_execution(
                QueryExecutionId = response_query_execution_id['QueryExecutionId']
                )
                status = response_get_query_details['QueryExecution']['Status']['State']
                
                if (status == 'FAILED') or (status == 'CANCELLED') :
                    failure_reason = response_get_query_details['QueryExecution']['Status']['StateChangeReason']
                    print(failure_reason)
                    return False, False

                elif status == 'SUCCEEDED':
                    location = response_get_query_details['QueryExecution']['ResultConfiguration']['OutputLocation']

                    ## Function to get output results
                    response_query_result = athena.get_query_results(
                        QueryExecutionId = response_query_execution_id['QueryExecutionId']
                    )
                    result_data = response_query_result['ResultSet']
                    
                    if len(response_query_result['ResultSet']['Rows']) > 1:
                        header = response_query_result['ResultSet']['Rows'][0]
                        rows = response_query_result['ResultSet']['Rows'][1:]
                    
                        header = [obj['VarCharValue'] for obj in header['Data']]
                        result = [dict(zip(header, get_var_char_values(row))) for row in rows]
        
                        return location, result
                    else:
                        return location, None
            else:
                    time.sleep(5)

            return False

    params = {
        'region': 'us-west-2',
        'database': 'ph_datasci-dev_db_data_lake',
        'bucket': 'annitest',
        'path': 'temp/athena/output',
        'query': "SELECT count(*) FROM act_created where cast(from_iso8601_timestamp(call_time) AS date) between cast(from_iso8601_timestamp('{start}') AS date) and cast(from_iso8601_timestamp('{end}')as date);".format(start=startDate, end=endDate)
    }


    ## Fucntion for obtaining query results and location 
    data = query_results(session3, params)

    #target table count
    actcreated_count= int(data[1][0]['_col0'])
    print("target_count: ", actcreated_count)




job.commit()