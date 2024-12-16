try:
    from logger import logger
    import pandas as pd
except ImportError as e:
    print(f"import error in tranformation: {e}")

class DATA_TRANSFORMATION:
    def __init__(self):
        pass
    def initiate_transformation(self,dataframe,sensor_id):
        try:
            """
            transformation methods
            """
            logger.info(f"transformation done for sensor_id :{sensor_id}")
            return dataframe
        except Exception as e:
            logger.error(f"Error transforming data: {e}", exc_info=True)
            return pd.DataFrame()
