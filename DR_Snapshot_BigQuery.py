import base64
from cronsim import CronSim
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta
from google.cloud.exceptions import NotFound
import db_dtypes
import  time
import logging
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession
from google.cloud import storage
import os
import  json
from datetime import datetime
project_id='premi0436563-gitenter'
datasets_list=[]
source_table=[]
dataset_id='procure_curated'
snap_dataset_id='procure_curated_snap'
dr_project_id='premi0582367-dc4prddr'
dr_dataset_id='procure_curated'

client=bigquery.Client()

datasets=list(client.list_datasets())

def get_snapshot_timestamp():
    cron_format="10 * * * *"
    it=CronSim(cron_format,datetime.now())
    next_interval=next(it)
    next_next_interval=next(it)
    delta=(next_next_interval-next_interval).total_seconds()
    prev_interval=next_interval-relativedelta(seconds=delta)
    prev_cron_interval_timestamp=int(prev_interval.timestamp()*1000)
    return prev_cron_interval_timestamp


def create_snapshot():
    seconds_before_expiration='604800'
    prev_cron_interval_timestamp=get_snapshot_timestamp()
    print(prev_cron_interval_timestamp)
    current_date=datetime.now().strftime("%Y%m%d")
    print(current_date)
    snapshot_expiration_date=datetime.now() + relativedelta(seconds=int(seconds_before_expiration))
    print(snapshot_expiration_date)
    for dataset in datasets:
        # print(dataset.dataset_id)
        dataset_to_search='procure_curated'
        if dataset_to_search in dataset.dataset_id :
            datasets_list.append(dataset.dataset_id)
    print(datasets_list)
    for dataset in datasets_list:
        tables=list(client.list_tables(dataset))
        for table in tables:
             # print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
             source_table.append(table.table_id)
    # print(source_table)
    # for table in source_table:
    #         snapshot_table_id=f"{project_id}.{dataset_id}.{table}_{current_date}"
    #         print(snapshot_table_id)
#
    for dataset in datasets_list:

        tables=list(client.list_tables(dataset))
        # for table in tables:
        #   table_to_be_skipped='snapshot'
        #   print(table.dataset_id)
        # if tables:
        for table in tables:
                    dataset_to_be_skipped='snapshot'
                    if dataset_to_be_skipped in table.dataset_id:
                      continue
                    dataset_snapshot_list=f"{project_id}.{table.dataset_id}_snapshot_{current_date}_{prev_cron_interval_timestamp}"
                    dataset_creation=bigquery.Dataset(dataset_snapshot_list)
                    dataset_creation.location='europe-west9'
                    # kms_key_name='projects/premi0436563-gitenter/locations/europe-west9/keyRings/datacloud-cstore-europe-west9-main-dev/cryptoKeys/datacloud-equery-universal-key'
                    # dataset_creation.default_encryption_configuration=kms_key_name
                    print("Found a dataset",dataset_snapshot_list)

                    try:
                        dataset_info=client.get_dataset(dataset_creation)
                    except NotFound:
                        dataset_info=client.create_dataset(dataset_creation)
                        # dataset_info.location='europe-west-9'

                    dataset_snap="{}".format(dataset_info.dataset_id)
                    table_dr_mig=bigquery.DatasetReference(project_id,dataset_snap)
                    table_snap_id=table_dr_mig.table(table.table_id)

                    print("Snapshot table Created",table_snap_id)
                    print("Source Table used ",f"{project_id}.{dataset_id}.{table.table_id}")
                    snapshot_table_id=f"{project_id}.{table_snap_id.dataset_id}.{table_snap_id.table_id}"
                    src_table=f"{project_id}.{table.dataset_id}.{table.table_id}"

                    try:
                        client.get_table(snapshot_table_id)  # Check if snapshot table exists
                        print(f"Snapshot table {snapshot_table_id} already exists. Skipping creation.")
                    except NotFound:
                        print(f"Snapshot table {snapshot_table_id} does not exist. Creating a new snapshot.")

                        copy_config=bigquery.CopyJobConfig()
                        copy_config.operation_type = "SNAPSHOT"
                    # copy_config.expires=datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
                    # days=5)
                        copy_config._properties["copy"]["destinationExpirationTime"]=snapshot_expiration_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                        copy_config.operation_type=bigquery.OperationType.SNAPSHOT
                        copy_job=client.copy_table(
                        sources=src_table,
                        destination=snapshot_table_id,
                        job_config=copy_config,
                        )
                        result=copy_job.result()
                    # print("Total number of rows {}",format(result.toal_rows))
                        print("Created table snapshot {} ".format(snapshot_table_id))
                    migrate_query=f""" 
                    Select * 
                    from `{table_snap_id.project}.{table_snap_id.dataset_id}.{table_snap_id.table_id}`
                    
                    """
                    original_query=f"""
                        select *
                        from `{table.project}.{table.dataset_id}.{table.table_id}`
                        
                        """
                    row_exec=client.query(migrate_query)
                    row_result=row_exec.result()
                    table_orig=client.get_table(table)
                    table_snap=client.get_table(table_snap_id)
                    print("Got Snap table {} rows".format(row_result.total_rows))
                    orig_schema=table_orig.schema
                    # print("Table Schema : {}".format(table_orig.schema))
                    columns_orig=[item for item in orig_schema]
                    original_query_exec=client.query(original_query)
                    orig_query_result=original_query_exec.result()
                    print("Got Original table {} rows ".format(orig_query_result.total_rows))
                    snap_schema=table_snap.schema
                    columns=[item for item in snap_schema]
                    column_lst=[col.name for col in columns]
                    result_columns=["{0} ".format(schema.name) for schema in snap_schema]
                    print("Columns to be displayed",result_columns)
                    # columns=",".join(str(columns))
                    snap_columns=[snap_detail for snap_detail in snap_schema]
                    # print("Snap shot Schema : {}".format(table_snap.schema))
                    # orig_query_df=orig_query_result.to_dataframe()
                    # rows=list(row_result)
                    # print("Desired Rows",rows)
                    # df_query=row_result.to_dataframe()
                    # print("--------------------------------------")
                    # print ("table.table_id: ", table.table_id)
                    # print ("type: ", type(table.table_id))
                    # print("--------------------------------------")
                    table_name=table.table_id
                    snap_table_id=table_snap_id.table_id
                    snap_dataset_id=table_snap_id.dataset_id
                    aggregated_columns = []
                    for col in columns:
                        if col.name != 'dc_created_timestamp':  # Don't aggregate dc_created_timestamp
                            aggregated_columns.append(f"MAX(snap.{col.name}) AS {col.name}")
                        else:
                            aggregated_columns.append(f"snap.{col.name}")  # Keep dc_created_timestamp without aggregation

                    group_by_columns = [f"snap.{col.name}" for col in columns if col.name != 'dc_created_timestamp']
                    group_by_columns.insert(0, "snap.dc_created_timestamp")  # Ensure dc_created_timestamp is first
                    merge_query=f'''     MERGE `{table.project}.{table.dataset_id}.{table.table_id}` AS dr
                    USING (
                        SELECT {', '.join(aggregated_columns)}
                        FROM `{snapshot_table_id}` AS snap
                        GROUP BY snap.dc_created_timestamp
                    ) AS snap
                    ON dr.dc_created_timestamp = snap.dc_created_timestamp
                    WHEN MATCHED THEN
                        UPDATE SET 
                            dr.dc_updated_timestamp = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN 
                        INSERT ({', '.join(column_lst)})
                        VALUES ({', '.join([f"snap.{col.name}" for col in columns])})
                    
                    '''
                    print(f"Generated merge query: {merge_query}")
                    snap_result=client.query(merge_query)
                    # snap_result.location='europe-west-3'
                    snap_result.result()
# def comparing_snapshots:
#     for i in source_table
create_snapshot()

# comparing_snapshots()

# source_table_id='premi0436563-gitenter.procure_dq.vms_subcon_active_dq_error_details'
# snapshot_table_id={'project_id'}join {'dataset_id'} 'snaps_vms_subcon_active_dq_error_details'



