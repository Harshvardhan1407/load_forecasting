try: 
    from logger import logger
    from LF_common_function import get_mongodb_connection, load_config, ensure_directory_exists, get_current_system_confgiuration
    from pipeline.ingestion import DATA_INGESTION
    from pipeline.transformation import DATA_TRANSFORMATION
    from concurrent.futures import ThreadPoolExecutor, as_completed, ProcessPoolExecutor
    from tenacity import retry, stop_after_attempt, wait_fixed
    import pandas as pd
    import os
    import sys
    import psutil
    import time
    from datetime import datetime

except ImportError as e:
    print(f"ImportError: {e}")

config = load_config()
etl_config = config['ETL']

available_memory,total_memory = get_current_system_confgiuration()

@retry(stop=stop_after_attempt(etl_config['retry_attempts']), wait=wait_fixed(etl_config['retry_wait_seconds']))
def etl_for_sensor(mongo_collection, sensor_id):
    try:
        start_time = time.time()
        # Your ETL code here

        logger.info(f"Starting ETL for sensor_id: {sensor_id}")
        data_ingestion_obj = DATA_INGESTION()
        validated_data = data_ingestion_obj.get_sensor_data(mongo_collection=mongo_collection, sensor_id=sensor_id)

        if validated_data.empty:
            logger.warning(f"No data extracted for sensor_id: {sensor_id}. Skipping.")
            return None
        
        data_tranformation_obj = DATA_TRANSFORMATION()
        transformed_data = data_tranformation_obj.initiate_transformation(validated_data,sensor_id)
        # logger.info(f"ETL completed successfully for sensor_id: {sensor_id}")
        logger.info(f"[Sensor_ID: {sensor_id}] ETL completed in {time.time() - start_time:.2f} seconds.")

        return transformed_data

    except Exception as e:
        logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)
        return None

def parallel_etl(mongo_collection,complete_data=True,max_workers=10):
    try:
        # MongoDB configuration
        if complete_data:
            total_count = mongo_collection.count_documents({})
            total_count2 = mongo_collection.estimated_document_count()
            logger.info(f"total data count in mongo_collection: {total_count}")
            logger.info(f"total data count in mongo_collection2: {total_count2}")
            single_field = mongo_collection.find_one()
            field_size = sys.getsizeof(single_field)
            total_size_mb = (field_size * total_count) / (1024 ** 2)
            logger.info(f"size: {field_size} bytes,total size: {total_size_mb:.2f} MB")
            # logger.info(f"stats: {mongo_collection.stats()}")
            
            pipeline = [
                {"$limit": 1},  # Fetch only one document
                {"$project": {"_id": 0, "bsonSize": {"$bsonSize": "$$ROOT"}}}  # Get BSON size of the document
            ]

            result = list(mongo_collection.aggregate(pipeline))

            if result:
                document_size = result[0]['bsonSize']
                total_size_mb = (document_size * total_count) / (1024 ** 2)
                logger.info(f"Actual BSON size of the document: {document_size} bytes,total size: {total_size_mb:.2f} MB")

                # logger.info(f" {document_size} bytes, \ntotal size:{format((document_size*total_count)/(1024*3),2)}mb")
            else:
                logger.info("No document found in the collection.")


        else:

            sensor_id_lst = mongo_collection.distinct('sensor')
            # Initialize an empty list to collect transformed data
            all_transformed_data = []


            # Use ThreadPoolExecutor for parallelism
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(etl_for_sensor, mongo_collection,  sensor_id): sensor_id for sensor_id in sensor_id_lst}
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

            output_file_path = config['ETL']['data_path']    
            # Ensure the output directory exists
            ensure_directory_exists(output_file_path)
            # Save the final DataFrame to a Parquet file
            output_file = os.path.join(output_file_path, f"transformed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
            # final_data.to_parquet(os.path.join(output_file_path, "complete_data.parquet"), index=False)
            final_data.to_parquet(output_file)
            # logger.info(f"ETL output saved to Parquet file at: {os.path.join(output_file_path, 'complete_data.parquet')}")
            logger.info(f"ETL output saved to Parquet file at: {output_file}")

            # else:
            #     logger.warning("No transformed data available to save.")
                
        logger.info("Parallel ETL pipeline completed.")

    except Exception as e:
        logger.error(f"Error in parallel ETL pipeline: {e}", exc_info=True)


if __name__ == "__main__":
    mongo_collection = get_mongodb_connection(purpose= "ingestion")
    parallel_etl(mongo_collection)
