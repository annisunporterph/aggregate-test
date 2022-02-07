

READ ME

The main goal of the exercise is to do the tests on the count of 'act' event created in the source json file were loaded in s3
and the count of target table 'act_created' registered in the catalog database within the time period of 
[StartDate, Enddate] (which can be extracted from the source json file)

Eg,
Target bronze table that we use Athena to query on:

        SELECT * FROM "ph_datasci-prod_db_data_lake"."act_created" 
        where cast(from_iso8601_timestamp(call_time) AS date)
        between cast(from_iso8601_timestamp('2022-01-23T00:00:00.000Z') AS date) 
        and cast(from_iso8601_timestamp('2022-01-23T23:59:59.999Z')as date)
        order by call_time desc

        ---return 18 


Source table that stored in s3(json formatted):
        Go to s3- buckets-click on one json file, find the start_date and end_date
        Query the s3
        "act": 18


        18=18 
        it matches

---------------------------------------------------------------------------------------        

1. Aggregate_test is the script I wrote to first make sure it could run locally.
your local machine credentials should be set up this way based on this article:
https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/configuration.html 

2. Aggregate_test_glue is the file i upload to the glue jobs and can be run on demand
once a while, the output will show on the CloudWatch output logs.
It has all the standard job parameters like all the other jobs. 

