try: 
    from logger import logger
    from component.LF_common_function import (
        get_mongodb_connection,
        load_config,
        ensure_directory_exists,
        get_current_system_confgiuration,
        calculate_collection_stats,
    )
    from component.ingestion import DATA_INGESTION
    from component.transformation import DATA_TRANSFORMATION
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tenacity import retry, stop_after_attempt, wait_fixed
    import pandas as pd
    import os
    import sys
    import time
    from datetime import datetime

except ImportError as e:
    print(f"ImportError: {e}")


class ETL_PIPELINE:
    def __init__(self):
        self.config = load_config()
        self.etl_config = self.config['ETL']
        self.available_memory, self.total_memory = get_current_system_confgiuration()
        self.output_file_path = self.etl_config['data_path']

    # @retry(stop=stop_after_attempt(3), wait=wait_fixed(5))
    def etl_for_sensor(self, mongo_collection, sensor_id):
        try:
            start_time = time.time()
            logger.info(f"Starting ETL for sensor_id: {sensor_id}")

            # Ingestion step
            data_ingestion_obj = DATA_INGESTION()
            validated_data = data_ingestion_obj.get_sensor_data(
                mongo_collection=mongo_collection, sensor_id=sensor_id
            )

            if validated_data.empty:
                logger.warning(f"No data extracted for sensor_id: {sensor_id}. Skipping.")
                return None

            # Transformation step
            data_transformation_obj = DATA_TRANSFORMATION()
            transformed_data = data_transformation_obj.initiate_transformation(validated_data, sensor_id)

            logger.info(f"[Sensor_ID: {sensor_id}] ETL completed in {time.time() - start_time:.2f} seconds.")
            return transformed_data

        except Exception as e:
            logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)
            return None
        
    def all_data_etl_pipeline(self, mongo_collection):
        try:
            data_ingeston_obj = DATA_INGESTION()
            data_transformation_obj = DATA_TRANSFORMATION()
            all_data = data_ingeston_obj.get_complete_data(mongo_collection)                
            validate_data = data_ingeston_obj.data_validation(all_data)
            transformed_data = data_transformation_obj.initiate_transformation(validate_data)
            return transformed_data
        
        except Exception as e:
            logger.info(f"error in complete data etl pipeline : {e}",exc_info=True)

    def save_transformed_data(self, transformed_data):
        try:
            # Ensure the output directory exists
            ensure_directory_exists(self.output_file_path)
            # Get today's date
            today_date = datetime.now().date()

            # List existing files with the same date
            existing_files = [
                f for f in os.listdir(self.output_file_path)
                if f.startswith(f"transformed_data_{today_date}")
            ]
            # Determine the file count and create a new filename with a suffix
            file_count = len(existing_files)
            output_file = os.path.join(
                self.output_file_path,
                f"transformed_data_{today_date}_{file_count + 1}.parquet"
            )

            # Save the DataFrame to the output file
            if isinstance(transformed_data, list):
                final_data = pd.concat(transformed_data, ignore_index=True)
            else:
                final_data = transformed_data

            estimated_size_mb = final_data.memory_usage(deep=True).sum() / (1024 ** 2)
            logger.info(f"Estimated size of transformed data: {estimated_size_mb:.2f} MB")
            # logger.info(f"approx data_size:{final_data.memory_usage(deep=True).sum()/(1024 ** 2)}")
            # final_data.to_parquet(output_file, index=False)

            logger.info(f"ETL output saved to Parquet file at: {output_file}")

        except Exception as e:
            logger.error(f"Error while saving transformed data: {e}", exc_info=True)

    def initiate_pipeline(self, mongo_collection, process_all=True, max_workers=10):
        try:
            if process_all:
                # mongo colection stats 
                total_size, total_count, avg_doc_size = calculate_collection_stats(mongo_collection)
                available_threads = os.cpu_count()
                logger.info(f"Number of threads: {available_threads}")
                # system configuration 
                self.available_memory,self.total_memoryf
                logger.info(f"half available memory: {self.available_memory/2}GB")
                logger.info(f"data size: {total_size} MB, available space in system: {self.available_memory/2} MB")
 
                if total_size < self.available_memory/2:
                    transformed_dataframe = self.all_data_etl_pipeline(mongo_collection)
                    logger.info(f"len of all_data :{len(transformed_dataframe)}")              
                else:
                    logger.debug("total size is greator than half available memory")
            else:
                transformed_complete_data = []
                sensor_id_lst = mongo_collection.distinct('sensor')

                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = {
                        executor.submit(self.etl_for_sensor, mongo_collection, sensor_id): sensor_id
                        for sensor_id in sensor_id_lst
                    }
                    for future in as_completed(futures):
                        sensor_id = futures[future]
                        try:
                            transformed_data = future.result()
                            if transformed_data is not None:
                                transformed_complete_data.append(transformed_data)
                        except Exception as e:
                            logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)
                transformed_dataframe = pd.concat(transformed_complete_data, ignore_index=True)

            # """
            # saving transformed_complete_data in parquet_file for further use
            # """
            # if not transformed_dataframe.empty:
            # # if transformed_complete_data:
            #     logger.info("All transformed data combined into a single DataFrame.")

            #     output_file = os.path.join(
            #         self.output_file_path,
            #         f"transformed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
            #     )
            #     ensure_directory_exists(self.output_file_path)

            #     transformed_dataframe.to_parquet(output_file, index=False)
            self.save_transformed_data(transformed_dataframe)
            # logger.info(f"ETL output saved to Parquet file at: {output_file}")
            # else:
            #     logger.warning("No transformed dataframe available to save.")

            logger.info("Parallel ETL pipeline completed.")

        except Exception as e:
            logger.error(f"Error in parallel ETL pipeline: {e}", exc_info=True)

    def test_pipeline():
        return {"etl pipeline working fine"}
    

# if __name__ == "__main__":
#     logger.info(f"##############################--ETL Pipeline start--######################################")
    
#     # Get the number of logical CPUs


#     etl_pipeline_obj = ETL_PIPELINE()

#     mongo_collection = get_mongodb_connection(purpose="ingestion")
#     etl_pipeline_obj.initiate_pipeline(mongo_collection)













# class ETL_PIPELINE:
#     def __init__(self):
#         self.config = load_config()
        
    
#     etl_config = config['ETL']

#     available_memory,total_memory = get_current_system_confgiuration()

#     @retry(stop=stop_after_attempt(etl_config['retry_attempts']), wait=wait_fixed(etl_config['retry_wait_seconds']))
#     def etl_for_sensor(mongo_collection, sensor_id):
#         try:
#             start_time = time.time()
#             # Your ETL code here

#             logger.info(f"Starting ETL for sensor_id: {sensor_id}")
#             data_ingestion_obj = DATA_INGESTION()
#             validated_data = data_ingestion_obj.get_sensor_data(mongo_collection=mongo_collection, sensor_id=sensor_id)

#             if validated_data.empty:
#                 logger.warning(f"No data extracted for sensor_id: {sensor_id}. Skipping.")
#                 return None
            
#             data_tranformation_obj = DATA_TRANSFORMATION()
#             transformed_data = data_tranformation_obj.initiate_transformation(validated_data,sensor_id)
#             # logger.info(f"ETL completed successfully for sensor_id: {sensor_id}")
#             logger.info(f"[Sensor_ID: {sensor_id}] ETL completed in {time.time() - start_time:.2f} seconds.")

#             return transformed_data

#         except Exception as e:
#             logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)
#             return None

#     def parallel_etl(mongo_collection,complete_data=True,max_workers=10):
#         try:
#             # MongoDB configuration
#             if complete_data:
#                 total_count = mongo_collection.count_documents({})
#                 total_count2 = mongo_collection.estimated_document_count()
#                 logger.info(f"total data count in mongo_collection: {total_count}")
#                 logger.info(f"total data count in mongo_collection2: {total_count2}")
#                 single_field = mongo_collection.find_one()
#                 field_size = sys.getsizeof(single_field)
#                 total_size_mb = (field_size * total_count) / (1024 ** 2)
#                 logger.info(f"size: {field_size} bytes,total size: {total_size_mb:.2f} MB")
#                 # logger.info(f"stats: {mongo_collection.stats()}")
                
#                 pipeline = [
#                     {"$limit": 1},  # Fetch only one document
#                     {"$project": {"_id": 0, "bsonSize": {"$bsonSize": "$$ROOT"}}}  # Get BSON size of the document
#                 ]

#                 result = list(mongo_collection.aggregate(pipeline))

#                 if result:
#                     document_size = result[0]['bsonSize']
#                     total_size_mb = (document_size * total_count) / (1024 ** 2)
#                     logger.info(f"Actual BSON size of the document: {document_size} bytes,total size: {total_size_mb:.2f} MB")

#                     # logger.info(f" {document_size} bytes, \ntotal size:{format((document_size*total_count)/(1024*3),2)}mb")
#                 else:
#                     logger.info("No document found in the collection.")


#             else:

#                 sensor_id_lst = mongo_collection.distinct('sensor')
#                 # Initialize an empty list to collect transformed data
#                 all_transformed_data = []


#                 # Use ThreadPoolExecutor for parallelism
#                 with ThreadPoolExecutor(max_workers=max_workers) as executor:
#                     futures = {executor.submit(etl_for_sensor, mongo_collection,  sensor_id): sensor_id for sensor_id in sensor_id_lst}
#                     for future in as_completed(futures):
#                         sensor_id = futures[future]
#                         try:
#                             transformed_data = future.result()
#                             if transformed_data is not None:
#                                 all_transformed_data.append(transformed_data)  # Append transformed data
#                         except Exception as e:
#                             logger.error(f"ETL failed for sensor_id {sensor_id}: {e}", exc_info=True)

#                 # Combine all transformed data into a single DataFrame
#                 if all_transformed_data:
#                     final_data = pd.concat(all_transformed_data, ignore_index=True)
#                     logger.info("All transformed data combined into a single DataFrame.")

#                 output_file_path = config['ETL']['data_path']    
#                 # Ensure the output directory exists
#                 ensure_directory_exists(output_file_path)
#                 # Save the final DataFrame to a Parquet file
#                 output_file = os.path.join(output_file_path, f"transformed_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet")
#                 # final_data.to_parquet(os.path.join(output_file_path, "complete_data.parquet"), index=False)
#                 final_data.to_parquet(output_file)
#                 # logger.info(f"ETL output saved to Parquet file at: {os.path.join(output_file_path, 'complete_data.parquet')}")
#                 logger.info(f"ETL output saved to Parquet file at: {output_file}")

#                 # else:
#                 #     logger.warning("No transformed data available to save.")
                    
#             logger.info("Parallel ETL pipeline completed.")

#         except Exception as e:
#             logger.error(f"Error in parallel ETL pipeline: {e}", exc_info=True)


# if __name__ == "__main__":
#     mongo_collection = get_mongodb_connection(purpose= "ingestion")
#     parallel_etl(mongo_collection)
