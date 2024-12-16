try: 
    from logger import logger
    from LF_common_function import get_mongodb_connection, load_config, ensure_directory_exists
    from pipeline.ingestion import DATA_INGESTION, DATA_VALIDATION
    from pipeline.transformation import DATA_TRANSFORMATION
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tenacity import retry, stop_after_attempt, wait_fixed
    import pandas as pd
    import os



except ImportError as e:
    print(f"ImportError: {e}")


@retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
def etl_for_sensor(mongo_collection, sensor_id, data_validation):
    try:
        logger.info(f"Starting ETL for sensor_id: {sensor_id}")
        data_ingestion_obj = DATA_INGESTION(validation=data_validation)
        validated_data = data_ingestion_obj.get_sensor_data(mongo_collection=mongo_collection, sensor_id=sensor_id)

        if validated_data.empty:
            logger.warning(f"No data extracted for sensor_id: {sensor_id}. Skipping.")
            return None
        
        data_tranformation_obj = DATA_TRANSFORMATION()
        transformed_data = data_tranformation_obj.initiate_transformation(validated_data,sensor_id)
        logger.info(f"ETL completed successfully for sensor_id: {sensor_id}")

        return transformed_data

    except Exception as e:
        logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)
        return None

def parallel_etl(max_workers=10):
    try:
        # MongoDB configuration
        mongo_collection = get_mongodb_connection(purpose= "ingestion")
        sensor_id_lst = mongo_collection.distinct('sensor')
        data_validation = DATA_VALIDATION()
        config = load_config()
        output_file_path = config['ETL']['data_path']    

        # Ensure the output directory exists
        ensure_directory_exists(output_file_path)


        # Initialize an empty list to collect transformed data
        all_transformed_data = []

        # Use ThreadPoolExecutor for parallelism
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(etl_for_sensor, mongo_collection,  sensor_id, data_validation): sensor_id for sensor_id in sensor_id_lst}
            for future in as_completed(futures):
                sensor_id = futures[future]
                try:
                    transformed_data = future.result()
                    if transformed_data is not None:
                        all_transformed_data.append(transformed_data)  # Append transformed data
                except Exception as e:
                    logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)

        # Combine all transformed data into a single DataFrame
        if all_transformed_data:
            final_data = pd.concat(all_transformed_data, ignore_index=True)
            logger.info("All transformed data combined into a single DataFrame.")
        
            # Save the final DataFrame to a Parquet file
            final_data.to_parquet(os.path.join(output_file_path, "complete_data.parquet"), index=False)
            logger.info(f"ETL output saved to Parquet file at: {output_file_path}")
        else:
            logger.warning("No transformed data available to save.")
            
        logger.info("Parallel ETL pipeline completed.")

    except Exception as e:
        logger.error(f"Error in parallel ETL pipeline: {e}", exc_info=True)


if __name__ == "__main__":
    parallel_etl()
