import json
import pandas as pd
from component.LF_common_function import load_schema
from logger import logger

def validate_dataframe_schema(df, schema):
    """
    Validate DataFrame columns and data types against the schema.
    """
    try:
        expected_columns = schema.get("COLUMNS", {})
        # Check if columns exist and their types match
        for col, expected_dtype in expected_columns.items():
            if col not in df.columns:
                raise ValueError(f"Missing column: {col}")
            actual_dtype = str(df[col].dtype)
            if actual_dtype != expected_dtype:
                raise ValueError(f"Column '{col}' has type '{actual_dtype}', expected '{expected_dtype}'")        
    except Exception as e:
        logger.error(f"Schema validation failed: {e}", exc_info=True)
        raise

class DATA_INGESTION:
    def __init__(self):
        self.schema = load_schema("schema.yaml")  # Load schema only once during class initialization
    
    def get_complete_data(self, mongo_collection):
        try:
            all_data = tuple(mongo_collection.find({}))
            if all_data:
                logger.info(f"Data ingestion of all sensor id done")
                df = pd.DataFrame(all_data)
                return df
            else:
                logger.error("no data found in mongo collection")
                return pd.DataFrame([])
        except Exception as e:
            logger.error(f"error in get complete data: {e}", exc_info= True)
    
    def data_validation(self,dataset):
        try:
            # Dynamically rename columns
            df = self.rename_columns(dataset)
            # Validate schema

            validate_dataframe_schema(df, self.schema)
            return df
        except Exception as e:
            logger.error(f"error in data validation: {e}", exc_info= True)

    def get_sensor_data(self, mongo_collection, sensor_id):
        try:
            """
            Extract data from MongoDB and validate its schema.
            """
            # Fetch Data
            data = list(mongo_collection.find({"sensor": sensor_id}, {"_id": 0}))
            if not data:
                logger.warning(f"No data found for sensor_id: {sensor_id}.")
                return pd.DataFrame()  # Return empty DataFrame for consistency

            # Convert to DataFrame
            df = pd.DataFrame(data)
            return df

        except Exception as e:
            logger.error(f"Error in DATA_INGESTION for sensor_id {sensor_id}: {e}", exc_info=True)
            return pd.DataFrame()

    def rename_columns(self, df):
        """
        Dynamically rename DataFrame columns based on schema.
        """
        try:
            rename_columns = self.schema.get("RENAME_COLUMNS", {})
            logger.info(f"colummns in dataframe: {tuple(df.columns)}")
            if "_id" in df.columns:
                df.drop("_id",axis=1, inplace= True)

            df.rename(columns=rename_columns, inplace=True)
            # logger.info("Columns renamed successfully.")
            return df
        except Exception as e:
            logger.error(f"Error in renaming columns: {e}", exc_info=True)
            raise
