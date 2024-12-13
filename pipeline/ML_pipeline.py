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

def main():
    # Load data
    ingestion = Ingestion()
    train, test = ingestion.load_data()
    logging.info("Data ingestion completed successfully")

    # Clean data
    cleaner = Cleaner()
    train_data = cleaner.clean_data(train)
    test_data = cleaner.clean_data(test)
    logging.info("Data cleaning completed successfully")

    # Prepare and train model
    trainer = Trainer()
    X_train, y_train = trainer.feature_target_separator(train_data)
    trainer.train_model(X_train, y_train)
    trainer.save_model()
    logging.info("Model training completed successfully")

    # Evaluate model
    predictor = Predictor()
    X_test, y_test = predictor.feature_target_separator(test_data)
    accuracy, class_report, roc_auc_score = predictor.evaluate_model(X_test, y_test)
    logging.info("Model evaluation completed successfully")
    
    # Print evaluation results
    print("\n============= Model Evaluation Results ==============")
    print(f"Model: {trainer.model_name}")
    print(f"Accuracy Score: {accuracy:.4f}, ROC AUC Score: {roc_auc_score:.4f}")
    print(f"\n{class_report}")
    print("=====================================================\n")

if __name__ == "__main__":
    main()
