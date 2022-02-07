import boto3
import json
import time

#for file 2022-01-24T01:00:12.294Z.json in s3, get the startDate and endDate from s3 select
sessions3 = boto3.Session(profile_name='datascience_dev')
s3 = sessions3.resource('s3')

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
        session = boto3.Session(profile_name='datascience_dev') 
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
    data = query_results(sessions3, params)

    #target table count
    actcreated_count= int(data[1][0]['_col0'])
    print("target_count: ", actcreated_count)


###################
## Function for cleaning up the query results to avoid redundant data
def clean_up():
    sessions3 = boto3.Session(profile_name='datascience_dev')
    s3 = sessions3.resource('s3')
    bucket = s3.Bucket('annitest')
    for obj in bucket.objects.filter(Prefix='Query-Results/'):
        s3.Object(bucket.name,obj.key).delete()

    for obj in bucket.objects.filter(Prefix='temp/'):
        s3.Object(bucket.name, obj.key).delete()

clean_up()


