try:
    import pandas as pd
    import numpy as np
    from logger import logger
    import os
    from dotenv import load_dotenv
    load_dotenv()
    from LF_common_function import get_mongodb_connection, load_schema,validate_dataframe_schema
    # get_mongodb_connection
    import json  # For parsing JSON strings
except ImportError as e:
    print(f"error in importing: {e}")


def data_ingestion():
    try:
        mongo_collection = get_mongodb_connection(purpose="ingestion")
        mongo_ingestion_query = os.getenv("ingestion_query")
        logger.info(f"mongo_query: {mongo_ingestion_query}")

        # Convert the JSON string to a dictionary
        if mongo_ingestion_query:
            mongo_ingestion_query = json.loads(mongo_ingestion_query)
        else:
            mongo_ingestion_query = {}  # Default to an empty query if no query is provided

        data = tuple(mongo_collection.find(mongo_ingestion_query, {"_id":0}))
                
        if not data:
            logger.error(f"No data found with query: {mongo_ingestion_query}")
        else:
            # print(f"len: {len(data)},\ndata: {data[0]}")
            return data
        return {}
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format for ingestion query: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in data_ingestion: {e}", exc_info=True)

def data_ingestion():
    try:
        # MongoDB Connection
        mongo_collection = get_mongodb_connection(purpose="ingestion")
        
        # Ingestion Query
        mongo_ingestion_query = os.getenv("ingestion_query")
        if mongo_ingestion_query:
            mongo_ingestion_query = json.loads(mongo_ingestion_query)
        else:
            mongo_ingestion_query = {}  # Empty query defaults to fetching all data

        # Fetch Data
        data = list(mongo_collection.find(mongo_ingestion_query, {"_id": 0}))
        if not data:
            logger.warning("No data found with the provided query.")
            return pd.DataFrame()  # Return empty DataFrame for consistency

        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Schema Validation
        schema = load_schema("schema.yaml")
        validate_dataframe_schema(df, schema)

        logger.info(f"Ingestion successful, fetched {len(df)} rows")
        return df

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON format in ingestion query: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Error in data_ingestion: {e}", exc_info=True)
        raise


data = data_ingestion()

