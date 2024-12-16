import json
import pandas as pd
from LF_common_function import load_schema
from logger import logger

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

            # logger.info("Schema validation passed")
        except Exception as e:
            logger.error(f"Error in validate_dataframe_schema: {e}", exc_info=True)
            raise

class DATA_INGESTION:
    def __init__(self, validation: DATA_VALIDATION):
        self.validation = validation
        self.schema = load_schema("schema.yaml")
        
    def get_sensor_data(self, mongo_collection, sensor_id):
        try:
            """
            extract data from MongoDB and validate its schema.
            """                        
            # Fetch Data
            data = list(mongo_collection.find({"sensor":sensor_id}, {"_id": 0}))
            if not data:
                logger.warning("No data found with the provided query.")
                return pd.DataFrame()  # Return empty DataFrame for consistency

            # Convert to DataFrame
            df = pd.DataFrame(data)
            validation_df = self.rename_columns(df)

            # Schema Validation
            self.validation.validate_dataframe_schema(validation_df, self.schema)

            logger.info(f"Ingestion successful, fetched {len(validation_df)} rows")
            return validation_df

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format in ingestion query: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error in data_ingestion: {e}", exc_info=True)

    def rename_columns(self,dataframe):
        try:
            rename_columns = {
                'sensor': 'sensor',
                'creation_time': 'creation_time',
                'R_Voltage': 'R_Voltage',
                'Y_Voltage': 'Y_Voltage',
                'B_Voltage': 'B_Voltage',
                'R_Current': 'R_Current',
                'Y_Current': 'Y_Current',
                'B_Current': 'B_Current',
                'KWh': "kWh",
                }
            dataframe.rename(columns=rename_columns, inplace=True)
            # logger.info(f"columns renamed")
            return dataframe
        
        except Exception as e:
            logger.error(f"error in renaming columns: {e}", exc_info= True)

# class DATA_INGESTION:
#     def __init__(self, validation: DATA_VALIDATION):
#         self.validation = validation
        
#     def ingest_data(self):
#         try:
#             """
#             Ingest data from MongoDB and validate its schema.
#             """            
#             mongo_collection = get_mongodb_connection(purpose="ingestion")
            
#             # Ingestion Query
#             mongo_ingestion_query = os.getenv("ingestion_query")
#             mongo_ingestion_query = json.loads(mongo_ingestion_query) if mongo_ingestion_query else {}


#             # Fetch Data
#             data = list(mongo_collection.find(mongo_ingestion_query, {"_id": 0}))
#             if not data:
#                 logger.warning("No data found with the provided query.")
#                 return pd.DataFrame()  # Return empty DataFrame for consistency

#             # Convert to DataFrame
#             df = pd.DataFrame(data)
#             validation_df = self.rename_columns(df)

#             # Schema Validation
#             schema = load_schema("schema.yaml")
#             self.validation.validate_dataframe_schema(validation_df, schema)

#             logger.info(f"Ingestion successful, fetched {len(validation_df)} rows")
#             return validation_df

#         except json.JSONDecodeError as e:
#             logger.error(f"Invalid JSON format in ingestion query: {e}", exc_info=True)
#         except Exception as e:
#             logger.error(f"Error in data_ingestion: {e}", exc_info=True)

#     def rename_columns(self,dataframe):
#         try:
#             rename_columns = {
#                 'sensor':'sensor',
#                 'creation_time':'creation_time',
#                 'R_Voltage': 'R_Voltage',
#                 'Y_Voltage': 'Y_Voltage',
#                 'B_Voltage': 'B_Voltage',
#                 'R_Current': 'R_Current',
#                 'Y_Current': 'Y_Current',
#                 'B_Current': 'B_Current',
#                 'KWh': "kWh",
#                 }
#             dataframe.rename(columns=rename_columns, inplace=True)
#             logger.info(f"columns renamed")
#             return dataframe
        
#         except Exception as e:
#             logger.error(f"error in renaming columns: {e}", exc_info= True)