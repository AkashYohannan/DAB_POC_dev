"""
===================================================================
version         Description            Author               Date
-------------------------------------------------------------------
.10             Initial Draft         Goutam Das          02/27/2025
.11             Rerun logic change    Ashwini Ramu        03/27/2025
===================================================================
"""
from pyspark.sql.functions import lit
from Framework.Libraries.Utils import generate_hash
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType
)


class Audit:
    """
    A PySpark-based audit management utility for tracking metadata across ETL steps.

    This class manages an audit Delta table that captures detailed metadata for
    job executions including job identifiers, statuses, checkpoint information,
    and timestamps. It supports functionality for inserting, retrieving, updating,
    and reprocessing audit records.

    The class helps maintain observability and traceability in distributed ETL pipelines
    by recording each task's progress and outcomes.

    :param spark: (SparkSession) The Spark session used for query and table operations.
    :param log_obj: (Logger) A structured logging object used for emitting and tracking logs.
    :param catalog: (str) The catalog name (default is "gfh_dev").
    :param schema: (str) The schema/database name (default is "audit").
    :param table: (str) The Delta table name (default is "ingestion_audit").
    """
    def __init__(self, spark, log_obj,  catalog="gfh_dev", schema="audit", table="ingestion_audit"):
        """Initialize Spark session and table references"""
        self.spark = spark
        self.log_obj = log_obj
        self.table = f"{catalog}.{schema}.{table}"
        self.columns_list = ','.join(self.spark.table(self.table).columns)

    def _get_last_entry(self, job_id, task_name, step):
        """
        Fetch the most recent record for a given job_id and task_name.

        Queries the audit table to retrieve the latest record
        based on `job_id` ,`task_name` and `step`, sorted by `created_at`.

        :param job_id: (str) Job execution identifier.
        :param task_name: (str) Unique run identifier.
        :param step: (str) Indicates Step number in Notebook

        :return: (dict) The latest record as a dictionary if found, else an empty dictionary.
        """
        try:
            query = f"""
                SELECT {self.columns_list} FROM {self.table}
                WHERE job_id = '{job_id}' AND task_name = '{task_name}' AND step = '{step}'
                ORDER BY created_at DESC LIMIT 1
            """
            result = self.spark.sql(query).collect()
            return result[0].asDict() if result else dict()
        except Exception as err:
            self.log_obj.log(log_level='ERROR', log_msg=f"Error fetching last_checkpoint: {err}")
            raise err

    def get_existing_audit_record(self, audit_id):
        """
           Fetch the most recent audit record for a given combination of job_id, run_id, and task_id.

           This method queries the audit table to retrieve the latest audit record
           associated with a specific `job_id`, `run_id`, and `task_id`.
           The results are ordered by `created_at DESC`, ensuring that the most recent
           record is returned if multiple entries exist.

           :param audit_id: (str) The unique identifier for an audit.

           :return: (dict | None) A dictionary containing the audit record's details
                    if found, otherwise `None`.
        """
        try:
            query = f"""
                SELECT {self.columns_list} FROM {self.table}
                WHERE audit_id = '{audit_id}'
            """
            result = self.spark.sql(query).collect()

            return result[0].asDict() if result else None  # Convert to dictionary
        except Exception as err:
            self.log_obj.log(log_level='ERROR', log_msg=f"Error Fetching audit record: {err}")
            raise err

    def insert_or_get_audit_record(self, **kwargs):
        """
        Insert new audit record if not exists, otherwise return the existing full record.

        This method creates a new audit record in the audit Delta table unless
        a previous incomplete record exists for the same job_id, run_id, task_name, and step.
        If a matching incomplete record is found, it is marked for reprocessing by updating
        its status and incrementing retry count.

        :param kwargs: (dict) All audit fields including:
                       job_id, run_id, task_name, step, job_name, audit_status,
                       source_id, dest_id, source_path, dest_path, reprocessed_count, etc.

        :return: (dict) The full inserted or reused audit record as a dictionary.
        """

        job_id = kwargs["job_id"]
        run_id = kwargs["run_id"]
        task_name = kwargs["task_name"]
        step = kwargs["step"]

        # Check if an audit record already exists
        latest_rec = self._get_last_entry(job_id, task_name, step)
        audit_status = latest_rec.get('audit_status')
        last_checkpoint_value = latest_rec.get('last_checkpoint')
        self.log_obj.log(log_level='INFO', log_msg=f"Status of latest entry :{audit_status}")
        if latest_rec and audit_status != "Success" and run_id == latest_rec['run_id']:
            self.log_obj.log_setup(audit_id=latest_rec['audit_id'])
            self.log_obj.log(log_level='INFO', log_msg="Existing audit record found")
            update_columns = {
                "audit_status": "Running",
                #"response_code": None,
                "reprocessed_count": latest_rec['reprocessed_count'] + 1,
                "completed_at": None
            }
            self.update_audit_record(latest_rec['audit_id'], **update_columns)
            self.log_obj.log(log_level='INFO', log_msg=f"Updated the audit record to reprocess")
            return latest_rec  # Return full record instead of just audit_id
        else:
            try:
                # Define the schema
                schema = StructType([
                    StructField("audit_id", StringType(), False),
                    StructField("config_tag", StringType(), True),
                    StructField("source_id", StringType(), False),
                    StructField("source_path", StringType(), True),
                    StructField("dest_id", StringType(), False),
                    StructField("dest_path", StringType(), True),
                    StructField("job_name", StringType(), False),
                    StructField("job_id", StringType(), False),
                    StructField("run_id", StringType(), False),
                    StructField("task_name", StringType(), False),
                    StructField("step", StringType(), False),
                    StructField("interface_pattern", StringType(), True),
                    StructField("load_type", StringType(), True),
                    StructField("source_count", IntegerType(), True),
                    StructField("audit_status", StringType(), False),
                    StructField("total_obj_count", IntegerType(), False),
                    StructField("loaded_obj_count", IntegerType(), False),
                    StructField("target_count", IntegerType(), True),
                    StructField("service_id", StringType(), False),
                    StructField("operation_type", StringType(), True),
                    StructField("reprocessed_count", IntegerType(), False),
                    StructField("audit_date", StringType(), False),
                    StructField("created_at", TimestampType(), False),
                    StructField("updated_at", TimestampType(), False),
                    StructField("completed_at", TimestampType(), True),
                    StructField("last_checkpoint", StringType(), True)
                ])

                # Generate a new audit_id
                new_audit_id = str(generate_hash(job_id, run_id, task_name, step))  # Generate unique audit_id
                new_audit_dict = {"audit_id": new_audit_id, **kwargs}

                # Convert new_audit_dict to DataFrame and append to Delta table
                audit_df = self.spark.createDataFrame([new_audit_dict], schema)

                # Add Checkpoint value
                audit_df = audit_df.withColumn("last_checkpoint", lit(last_checkpoint_value))

                audit_df.write.format("delta").mode("append").saveAsTable(self.table)
                df_dict = audit_df.collect()[0].asDict()

                self.log_obj.log_setup(audit_id=new_audit_id)
                self.log_obj.log(log_level='INFO', log_msg="New audit record inserted")
                return df_dict

            except Exception as err:
                self.log_obj.log(log_level='ERROR', log_msg=f"Error inserting audit record: {err}")
                raise err


    def update_audit_record(self, audit_id, **kwargs):
        """
        Update an existing audit record in the audit table.

        This method updates the specified columns for a given audit_id and
        automatically updates the `Updated_at` timestamp to reflect the modification time.

        :param audit_id: (str) The unique identifier of the audit record to update.
        :param kwargs: (dict) Column names and their updated values (key-value pairs).

        :return: None
        """
        try:
            set_clause = ', '.join(
                [
                    f"{key} = {value}" if isinstance(value, str) and value.lower() == "current_timestamp()"  # Handle SQL functions
                    else f"{key} = NULL" if value is None  # Convert None to NULL
                    else f"{key} = '{value}'"  # Quote other values
                    for key, value in kwargs.items()
                ]
            )
            update_query = f"""
                UPDATE {self.table}
                SET {set_clause}, Updated_at = current_timestamp()
                WHERE audit_id = '{audit_id}'
            """
            self.spark.sql(update_query)
            self.log_obj.log(log_level='INFO', log_msg=f"Audit record {audit_id} updated successfully")
        except Exception as err:
            self.log_obj.log(log_level='ERROR', log_msg=f"Error updating audit record: {err}")
            raise err
