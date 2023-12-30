import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from pyspark.sql import SQLContext
import sys
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import re
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
FEED_CONFIG_KEY = "feed_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'raw_to_access'
JOB_KEY = "raw-to-access"


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_KEY,
                               REGION_KEY])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger = glueContext.get_logger()

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    # pre_raw_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-raw-to-access')
    log_manager.log(message="Starting the raw-to-access job",
                    args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                          'access_db': athena_access_database, "job": JOB_KEY})

    date_table = feed_config.get('transformation-rules', 'table-name')
    delete_date = feed_config.get('transformation-rules', 'delete-date')
    delete_table = feed_config.get('transformation-rules', 'delete-table')
    source_system = feed_config.get('transformation-rules', 'src-sys-abbr')
    pgdb_table = feed_config.get('transformation-rules', 'pgdb-table')

    print(date_table)
    pgdb_table_split = pgdb_table.split('_')
    pgdb_table_third = pgdb_table_split[1]
    print(pgdb_table_third)

    if pgdb_table_third == 'fulfillment':
        refertable = glueContext.create_dynamic_frame.from_catalog(database=athena_access_database,
                                                                   table_name=date_table,
                                                                   transformation_ctx="refertable")
        print("refertable")
        refertable.show()

        applymapping = ApplyMapping.apply(frame=refertable, mappings=[("latest_date", "date", "latest_date", "date"),
                                                                      ("file_name", "string", "file_name", "string"),
                                                                      ("current_date", "date", "current_date", "date"),
                                                                      ("mode", "string", "mode", "string"), (
                                                                          "src_sys_abbr", "string", "src_sys_abbr",
                                                                          "string")], transformation_ctx="applymapping")
        print("applymapping")
        applymapping.show()

        date_df = applymapping.toDF()
        date_df_count = date_df.count()
        print("total count is : " + str(date_df_count))
        delete_condition = 'latest_date <> ' + delete_date + ' and src_sys_abbr = "PHYSICAL" '
        print(delete_condition)
        date_df_updated = date_df.where(delete_condition)
        print(date_df_updated)
        updated_table_count = date_df_updated.count()
        print("updated count is : " + str(updated_table_count))

        date_df_updated.registerTempTable("dateDfUpdated")
        athena_write_df = spark.sql(
            "INSERT OVERWRITE TABLE " + athena_access_database + "." + date_table + " partition (src_sys_abbr= 'PHYSICAL') select /*+ COALESCE(20) */ latest_date, file_name, current_date, mode from dateDfUpdated")

        if athena_write_df is 200:
            log_manager.log(message='Athena write query executed successfully',
                            args={'HTTPStatusCode': str(athena_write_df)})

        delete_condition1 = 'latest_date <> ' + delete_date + ' and src_sys_abbr = "ELECTRONIC" '
        print(delete_condition1)
        date_df_updated1 = date_df.where(delete_condition)
        print(date_df_updated1)
        updated_table_count1 = date_df_updated1.count()
        print("updated count is : " + str(updated_table_count1))

        date_df_updated.registerTempTable("dateDfUpdated1")
        athena_write_df1 = spark.sql(
            "INSERT OVERWRITE TABLE " + athena_access_database + "." + date_table + " partition (src_sys_abbr= 'ELECTRONIC') select /*+ COALESCE(20) */ latest_date, file_name, current_date, mode from dateDfUpdated1")

        if athena_write_df1 is 200:
            log_manager.log(message='Athena write query executed successfully',
                            args={'HTTPStatusCode': str(athena_write_df)})
    else:
        refertable = glueContext.create_dynamic_frame.from_catalog(database=athena_access_database,
                                                                   table_name=date_table,
                                                                   transformation_ctx="refertable")
        print("refertable")
        refertable.show()

        applymapping = ApplyMapping.apply(frame=refertable, mappings=[("latest_date", "date", "latest_date", "date"),
                                                                      ("batch_date", "date", "batch_date", "date"),
                                                                      ("current_date", "date", "current_date", "date"),
                                                                      ("src_sys_abbr", "string", "src_sys_abbr",
                                                                       "string"),
                                                                      ("table_name", "string", "table_name", "string")],
                                          transformation_ctx="applymapping")
        print("applymapping")
        applymapping.show()

        date_df = applymapping.toDF()
        date_df_count = date_df.count()
        print("total count is : " + str(date_df_count))
        delete_condition = 'batch_date <> ' + delete_date + ' and table_name = ' + delete_table + ' and src_sys_abbr = ' + source_system
        print(delete_condition)
        date_df_updated = date_df.where(delete_condition)
        print(date_df_updated)
        updated_table_count = date_df_updated.count()
        print("updated count is : " + str(updated_table_count))

        date_df_updated.registerTempTable("dateDfUpdated")
        athena_write_df = spark.sql(
            "INSERT OVERWRITE TABLE " + athena_access_database + "." + date_table + " partition (table_name= " + delete_table + " , src_sys_abbr= " + source_system + ") select /*+ COALESCE(20) */ latest_date, batch_date, current_date from dateDfUpdated")

        if athena_write_df is 200:
            log_manager.log(message='Athena write query executed successfully',
                            args={'HTTPStatusCode': str(athena_write_df)})

    job.commit()


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()