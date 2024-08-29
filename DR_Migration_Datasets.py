from google.cloud import bigquery,storage
from google.cloud.exceptions import NotFound
import json
import pandas as pd
from pyarrow  import parquet
client=bigquery.Client()
bucket_config='datacloud-finance-temporary-dev'
blob_name='migration-data'
bucket_name='datacloud-finance-temporary-dev/migration-data'
project_id='premi0436563-gitenter'
# dataset_id_list=['gtd_cleansed','gtd_curated','gtd_dq','gtd_raw']
datasets_list=[]
# dataset_id='hr_config'

project=client.project
datasets=list(client.list_datasets())
for dataset in datasets:
    # print(dataset.dataset_id)
    dataset_to_search='hr'
    if dataset_to_search in dataset.dataset_id :
        datasets_list.append(dataset.dataset_id)
print(datasets_list)
#print(datasets)
# if datasets:
#     print("Datasets in a project {} :".format(project))
#     for dataset in datasets:
#         print("\t {} .".format(dataset.dataset_id))
for dataset_list in datasets_list:
    tables=list(client.list_tables(dataset_list))
    for table in tables:
         print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    if tables:
        for table in tables:
            project_dr='premi0582367-dc4prddr'
            print("Datasets in project {}:".format(project_dr))
            # dataset_name="{}.{}".format('premi0582367-dc4prddr',f"{dataset_id}")
            dataset_name="{}.{}".format('premi0582367-dc4prddr',f"{table.dataset_id}")
            dataset_creation=bigquery.Dataset(dataset_name)
            dataset_creation.location='europe-west3'
            print(dataset_name)
            try:
                dataset_info=client.get_dataset(dataset_creation)
                # dataset_info.location="europe-west3"
            except NotFound:
                dataset_info=client.create_dataset(dataset_creation)
                # dataset_info.location="europe-west3"
            dataset_mig="{}".format(dataset_info.dataset_id)
            print('Dataset to be migrated',dataset_mig)
            file_name_end_with='dq_summary'
            file_name_error_details='error_details'
            print("Table to be migrated",table.table_id)
            view_to_be_skipped='vw'
            if view_to_be_skipped in table.table_id:
                continue
            if file_name_end_with in table.table_id:
                destination_uri="gs://{}/{}".format(bucket_name,f"{table.table_id}*.json")
            else:
                destination_uri="gs://{}/{}".format(bucket_name,f"{table.table_id}*.parquet")
            print("Destination URI",destination_uri)
            # json_file=f"{table.table_id}*.json"
            # print(json_file)
            dataset_ref=bigquery.DatasetReference(project_id,table.dataset_id)
            table_ref=dataset_ref.table(table.table_id)
            table_dr_mig=bigquery.DatasetReference(project_dr,dataset_mig)
            table_dr_id=table_dr_mig.table(table.table_id)
            print(table_dr_id)
            print(table_ref)
            print("\n")
            print(destination_uri)

            if file_name_end_with in destination_uri:
                print("yes for Json")
                job_config=bigquery.ExtractJobConfig()
                job_config.destination_format=(bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON)
            #job_config.autodetect = True
                extract_job=client.extract_table(table_ref,
                                  destination_uri,
                                             job_config=job_config,
                                  )
                extract_job.result()

            if file_name_error_details in destination_uri:
                print("yes for Parquet")
                job_config=bigquery.ExtractJobConfig()
                job_config.destination_format=(bigquery.DestinationFormat.PARQUET)
            #job_config.autodetect = True
                extract_job=client.extract_table(table_ref,
                                  destination_uri,
                                             job_config=job_config,
                                  )
                extract_job.result()

            if file_name_end_with not in destination_uri and file_name_error_details not in destination_uri :
            # else:
                 job_config=bigquery.ExtractJobConfig()
                 print("yes for others")
                 job_config.destination_format=(bigquery.DestinationFormat.PARQUET)
                 # job_config.autodetect = True
                 extract_job=client.extract_table(table_ref,
                                  destination_uri,
                                             job_config=job_config,
                                  )
                 extract_job.result()
            print("Exported {}:{}.{} to {}".format(project_id,table.dataset_id,table.table_id,destination_uri))
            # #table_id=
            # # str_schema=json.dumps(destination_uri)
            # #str_schema=str(destination_uri)
            # client=storage.Client()
            # bucket=client.get_bucket('datacloud-finance-temporary-dev')
            # blob=bucket.list_blobs()
            # print('Blob details',blob)
            # for line in blob:
            #     line=line.name.split('/')[-1]
            #     print('Final File Loaded',line)
            #     bigquery_schema=[]
            #     with open (line,'r') as f:
            #         bigqueryColumns=json.load(f)
            #         for col in bigqueryColumns:
            #             bigquery_schema.append(bigquery.SchemaField(col['name'],col['type']))
            # print(bigquery_schema)


            # schema=client.schema_from_json(destination_uri)
            schema = [
            bigquery.SchemaField('data_quality_scan', 'RECORD', mode='REPEATED',fields=[
            bigquery.SchemaField('resource_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('project_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('location', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('data_scan_id', 'STRING', mode='NULLABLE'),

            ]),
		    bigquery.SchemaField('data_source', 'RECORD', mode='REPEATED',fields=[
            bigquery.SchemaField('resource_name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('dataplex_entity_project_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('dataplex_entity_project_number', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('dataplex_lake_id', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('dataplex_zone_id', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('dataplex_entity_id', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('table_project_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('table_project_number', 'INTEGER', mode='NULLABLE'),
			bigquery.SchemaField('dataset_id', 'STRING', mode='NULLABLE'),
			bigquery.SchemaField('table_id', 'STRING', mode='NULLABLE'),
            ]),
            bigquery.SchemaField('data_quality_job_id', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('data_quality_job_configuration', 'JSON', mode='NULLABLE'),
            bigquery.SchemaField('job_labels', 'JSON', mode='NULLABLE'),
		    bigquery.SchemaField('job_start_time', 'TIMESTAMP', mode='NULLABLE'),
		    bigquery.SchemaField('job_end_time', 'TIMESTAMP', mode='NULLABLE'),
            bigquery.SchemaField('job_quality_result', 'RECORD', mode='REPEATED', fields=[
            bigquery.SchemaField('passed', 'BOOLEAN', mode='NULLABLE'),
            bigquery.SchemaField('score', 'FLOAT', mode='NULLABLE'),

            ]),
		     bigquery.SchemaField('job_dimension_result', 'JSON', mode='NULLABLE'),
             bigquery.SchemaField('job_rows_scanned', 'INTEGER', mode='NULLABLE'),
		     bigquery.SchemaField('rule_name', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_description', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_type', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_evaluation_type', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_column', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_dimension', 'STRING', mode='NULLABLE'),
		     bigquery.SchemaField('rule_threshold_percent', 'FLOAT', mode='NULLABLE'),
		     bigquery.SchemaField('rule_parameters', 'JSON', mode='NULLABLE'),
		     bigquery.SchemaField('rule_passed', 'BOOLEAN', mode='NULLABLE'),
		     bigquery.SchemaField('rule_rows_evaluated', 'INTEGER', mode='NULLABLE'),
		     bigquery.SchemaField('rule_rows_passed', 'INTEGER', mode='NULLABLE'),
		     bigquery.SchemaField('rule_rows_passed_percent', 'FLOAT', mode='NULLABLE'),
		     bigquery.SchemaField('rule_rows_null', 'INTEGER', mode='NULLABLE'),
		     bigquery.SchemaField('rule_failed_records_query', 'STRING', mode='NULLABLE'),

             ]
            if file_name_end_with in destination_uri:
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    schema=schema
                 )
                load_job=client.load_table_from_uri(
                    destination_uri,table_dr_id,
                    job_config=job_config,
                # schema=schema
                    )
                load_job.result()

            if file_name_error_details in destination_uri:
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    # schema=schema
                 )
                load_job=client.load_table_from_uri(
                    destination_uri,table_dr_id,
                     job_config=job_config,
                # schema=schema
                    )
                load_job.result()

            if file_name_end_with not in destination_uri and file_name_error_details not in destination_uri :
                job_config=bigquery.LoadJobConfig(
                    source_format=bigquery.SourceFormat.PARQUET,
                    # schema=schema
                 )
                load_job=client.load_table_from_uri(
                    destination_uri,table_dr_id,job_config=job_config,
                # schema=schema
                    )
                load_job.result()
            storage_client=storage.Client()
            bucket=storage_client.get_bucket(bucket_config)
            blob=bucket.list_blobs(prefix=blob_name)
            print(blob)
            for blobs in blob:
                blobs.delete()
            destination_table=client.get_table(table_dr_id)
            # destination_load=[]
            # destination_load.append(destination_uri)
                        # dataset_parquet_load=parquet.ParquetDataset(destination_uri)
            # df=dataset_parquet_load.read().to_pandas()
# def bucket_data_load():
#         client=storage.Client()
#         bucket=client.get_bucket('datacloud-finance-temporary-dev')
#         blob=bucket.list_blobs()
#         print('Blob details',blob)
#         for line in blob:
#                 line=line.name.split('/')[-1]
#                 print('Final File Loaded',line)
#                 return line
