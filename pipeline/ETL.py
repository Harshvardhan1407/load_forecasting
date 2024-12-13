from concurrent.futures import ThreadPoolExecutor
import os
from dotenv import load_dotenv
load_dotenv()
import json
import concurrent
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from bson import ObjectId
from LF_common_function import get_mongodb_connection, load_schema
from logger import logger
# import mongo_query.datatransformation as datatransformation
# import mongo_query.sensor_ids as sensor_ids
# from config.db_port import get_database
# from logs import logs_config


mongo_collection = get_mongodb_connection()

class DATA_VALIDATION:
    @staticmethod

    def validate_dataframe_schema(df, schema):
        try:
            """
            Validate if DataFrame adheres to the schema.
            """
            expected_columns = schema.get("COLUMNS", {})
            
            for col, dtype in expected_columns.items():
                if col not in df.columns:
                    raise ValueError(f"Missing column: {col}")
                if str(df[col].dtype) != dtype:
                    raise ValueError(f"Column {col} has invalid type. Expected {dtype}, found {df[col].dtype}")

            logger.info("Schema validation passed")
        except Exception as e:
            logger.error(f"Error in validate_dataframe_schema: {e}", exc_info=True)
            raise

class DATA_INGESTION:
    def __init__(self, validation: DATA_VALIDATION):
        self.validation = validation
        
    def ingest_data(self):
        try:
            """
            Ingest data from MongoDB and validate its schema.
            """            
            mongo_collection = get_mongodb_connection(purpose="ingestion")
            
            # Ingestion Query
            mongo_ingestion_query = os.getenv("ingestion_query")
            mongo_ingestion_query = json.loads(mongo_ingestion_query) if mongo_ingestion_query else {}


            # Fetch Data
            data = list(mongo_collection.find(mongo_ingestion_query, {"_id": 0}))
            if not data:
                logger.warning("No data found with the provided query.")
                return pd.DataFrame()  # Return empty DataFrame for consistency

            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Schema Validation
            schema = load_schema("schema.yaml")
            self.validation.validate_dataframe_schema(df, schema)

            logger.info(f"Ingestion successful, fetched {len(df)} rows")
            return df

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in ingestion query: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in data_ingestion: {e}", exc_info=True)

class DATA_TRANSFORMATION:
    def initiate_tranformation():
        try:
            logger.info(f"transformation done")
        except Exception as e:
            logger.error(f"Error in initiate_tranformation: {e}", exc_info=True)


# def data_fetch(sensor_id, site_id):
#     try:
#         from_id = f"{sensor_id}-2024-01-01 00:00:00"
#         to_id = f"{sensor_id}-2024-03-31 23:59:59"
#         query = {"_id": {"$gte": from_id, "$lt": to_id}}

#         results = list(sensor.find(query))
#         for doc in results:
#             if '_id' in doc:
#                 doc['_id'] = str(doc['_id'])

#         if results:
#             df = datatransformation.init_transformation(results, site_id)
#             if df is None:
#                 logs_config.logger.info(f"Nothing transformed for sensor_id: {sensor_id}")
#             else:
#                 logs_config.logger.info(f"Fetched and transformed {len(df)} records for sensor_id: {sensor_id}")
#             return df
#         else:
#             logs_config.logger.info(f"No records found for sensor_id: {sensor_id}")
#             return None

#     except Exception as e:
#         logs_config.logger.error(f"Error fetching data for sensor_id {sensor_id}: {e}", exc_info=True)
#         return None

# def fetch_data_for_sensors(circle_id, output_dir="sensor_data"):
#     os.makedirs(output_dir, exist_ok=True)

#     sensors = sensor_ids.sensor_ids(circle_id)
#     sensorids = [doc["id"] for doc in sensors]
#     site_ids = [doc["site_id"] for doc in sensors]

#     all_dicts = []
#     with ThreadPoolExecutor() as executor:
#         futures = {executor.submit(data_fetch, sensor_id, site_id): (sensor_id, site_id) for sensor_id, site_id in zip(sensorids, site_ids)}

#         for future in concurrent.futures.as_completed(futures):
#             dicts = future.result()
#             if dicts is not None and len(dicts) > 0:
#                 all_dicts.extend(dicts)

#     if all_dicts:
#         combined_df = pd.DataFrame(all_dicts)
        
#         # Convert all object type columns to strings if necessary
#         combined_df = combined_df.applymap(lambda x: str(x) if isinstance(x, ObjectId) else x)

#         table = pa.Table.from_pandas(combined_df)
#         pq.write_table(table, os.path.join(output_dir, f"{circle_id}_data.parquet"))
        
#         logs_config.logger.info(f"Saved data for circle_id: {circle_id}")
#         return "saved"
#     else:
#         logs_config.logger.info(f"No data to save for circle_id: {circle_id}")
#         return "no data"

