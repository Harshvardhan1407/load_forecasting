try:
    from logger import logger
    import pandas as pd
    import holidays
    import matplotlib.pyplot as plt
    from datetime import timedelta, datetime
    import seaborn as sns

except ImportError as e:
    print(f"import error in tranformation: {e}")

class DATA_TRANSFORMATION:
    def __init__(self):
        pass
    
    def initiate_transformation(self,dataframe,sensor_id):
        try:
            def basic_chceks():
                year = 2022
                start_month = None
                end_month = 5
                """
                transformation methods
                """
                df = dataframe.copy()
                df.drop("sensor", axis=1, inplace=True)
                # print(f"df rows: {len(df)}")
                # print(f"Duplicated rows: {df.duplicated().sum()}")
                # df.set_index("creation_time", drop=True, inplace=True)
                df = df.loc[df.index.year>year]
                df = df.loc[~ ((df.index.year== year) & (df.index.month > end_month))]

                # print(f"Duplicated rows after datetime index: {df.duplicated().sum()}")
                df = df.drop_duplicates()
                df = df[df['R_Voltage'] != 0]
                # df = df.loc[~df['R_Voltage'] == 0]
                # print(f"Duplicated index: {df.index.duplicated().sum()}")
                # print(f"null_values : {df.isna().sum().sum()}")
                # print(f"df rows after basic checks: {len(df)}")

            def initial_validation(df):
                resample_df = df[['KWh']].resample(rule="30min").asfreq()
                print(f"null vlaues after resampling: {resample_df.isna().sum().sum()}")
                # resample_df[resample_df.isna().any(axis=1)]
                resample_df = resample_df.interpolate(method="linear")
                print(f"null values after filling: {resample_df.isna().sum().sum()}")
                resample_df['KWh'] = resample_df['KWh'].rolling(window=4).mean()
                resample_df['KWh'].plot()
                plt.show()
                return resample_df

            def data_ingestion_training(month):
                df_complete_data = pd.read_parquet('HT_meter_complete_data_9_id.parquet')
                df = df_complete_data[df_complete_data['sensor']=="6148740eea9db0.29702291"]  ## Jamia HT METER
                df = df.copy()

                df.drop("sensor", axis=1, inplace=True)
                print(f"df rows: {len(df)}")
                print(f"Duplicated rows: {df.duplicated().sum()}")
                df.set_index("creation_time", drop=True, inplace=True)
                df = df.loc[df.index.year>2022]
                df = df.loc[((df.index.year==2024) & ((df.index.month>=(month-2)) & (df.index.month<(month+1))))]

                print(f"Duplicated rows after datetime index: {df.duplicated().sum()}")
                df = df.drop_duplicates()
                df = df[df['R_Voltage'] != 0]
                # df = df.loc[~df['R_Voltage'] == 0]
                print(f"Duplicated index: {df.index.duplicated().sum()}")
                print(f"null_values : {df.isna().sum().sum()}")
                print(f"df rows after basic checks: {len(df)}")
                return df

            def pre_process(data):
                main_df = data[['KWh']].resample(rule="30min").asfreq()
                print(f"null vlaues after resampling: {main_df.isna().sum().sum()}")
                # resample_df[resample_df.isna().any(axis=1)]
                main_df = main_df.interpolate(method="linear")
                print(f"null values after filling: {main_df.isna().sum().sum()}")
                # resample_df['KWh'] = resample_df['KWh'].rolling(window=4).mean()
                main_df['KWh'].plot()
                # plt.show()
                return main_df

            def holidays_list(start_date, end_date):
                holiday_list = []
                india_holidays = holidays.CountryHoliday('India', years=start_date.year)
                # Iterate through each date from start_date to end_date
                current_date = start_date
                while current_date <= end_date:
                    # Check if the current date is a holiday in India or a Sunday
                    if current_date in india_holidays or current_date.weekday() == 6:
                        holiday_list.append(current_date)
                    current_date += timedelta(days=1)

                return holiday_list


            def correlation_matrix(df):
                correlation_matrix_data = df.corr()
                # Set up the matplotlib figure
                plt.figure(figsize=(18, 10))
                # Draw the heatmap
                sns.heatmap(correlation_matrix_data, annot=True, fmt=".2f", cmap='coolwarm', square=True, linewidths=0.5)
                # Show the plot
                plt.show()

            def add_lags(dff, target_col, large_data= False):
                # target_map = dff['consumed_unit'].to_dict()
                target_map = dff[target_col].to_dict()
                dff['rolling_4'] = dff['KWh'].rolling(window=4).mean()

                # 1 Hour, 2 Hours, 6 Hours
                dff['lag1_hour']   =  (dff.index - pd.Timedelta('1 hour')).map(target_map)
                dff['lag2_hours']  =  (dff.index - pd.Timedelta('2 hours')).map(target_map)
                dff['lag3_hours']  =  (dff.index - pd.Timedelta('3 hours')).map(target_map)
                # dff['lag6_hours']  =  (dff.index - pd.Timedelta('6 hours')).map(target_map)
                # dff['lag12_hours'] =  (dff.index - pd.Timedelta('12 hours')).map(target_map)
                dff['lag1_day']    =  (dff.index - pd.Timedelta('1 day')).map(target_map)
                # dff['lag2_days']   =  (dff.index - pd.Timedelta('2 days')).map(target_map)
                # dff['lag3_days']   =  (dff.index - pd.Timedelta('3 days')).map(target_map)
                dff['lag7_days']   =  (dff.index - pd.Timedelta('7 days')).map(target_map)

                # if large_data:
                # dff['lag_15_day'] = (dff.index - pd.Timedelta('15 days')).map(target_map)
                dff['lag_30_day'] = (dff.index - pd.Timedelta('30 days')).map(target_map)
                # dff['lag_45_day'] = (dff.index - pd.Timedelta('45 days')).map(target_map)
                    # logger.info(f" lags added for large data")
                #     return dff
                # else:
                
                dff['daily_avg'] = dff[target_col].rolling(window=48).mean()  # 24-hour rolling mean
                dff['weekly_avg'] = dff[target_col].rolling(window=7*48).mean()  # Weekly rolling mean


                # logger.info(f"lags added")
                return dff



            def create_features(hourly_data,lagre_data= False):

                hourly_data = hourly_data.copy()
                # Check if the index is in datetime format
                if not isinstance(hourly_data.index, pd.DatetimeIndex):
                    hourly_data.index = pd.to_datetime(hourly_data.index)
                    
                hourly_data['hour'] = hourly_data.index.hour
                hourly_data['day'] = hourly_data.index.day
                hourly_data['dayofweek'] = hourly_data.index.dayofweek
                hourly_data['weekofyear'] = hourly_data.index.isocalendar().week
                # hourly_data['dayofyear'] = hourly_data.index.dayofyear
                # hourly_data['is_weekend'] = hourly_data['dayofweek'].isin([5, 6]).astype(int)
                hourly_data['holiday'] = 0
                # if lagre_data:
                # hourly_data['month'] = hourly_data.index.month
                # hourly_data['quarter'] = hourly_data.index.quarter
                # hourly_data['weekofyear'] = hourly_data.index.isocalendar().week
                # hourly_data['year'] = hourly_data.index.year

                return hourly_data

            def feature_engineering(dataframe, c_matrix=False):
                weather_df = pd.read_parquet("weather_data_2024_10_24.parquet")
                # latitude, longitude = "28.46072","77.537381" #"28.625361","77.376214"#, 28.628059,77.378912
                from_date = dataframe.first_valid_index().date()
                to_date = dataframe.last_valid_index().date()
                print("from_date: ",from_date,"to_date: ",to_date)

                holidays_lst= holidays_list(from_date,to_date)
                print("holidays_lst:",holidays_lst)
                df_lags = add_lags(dataframe,target_col="KWh")
                df_features = create_features(df_lags)
                for date in holidays_lst:
                    df_features.loc[f"{date}", 'holiday'] = 1
                final_df = df_features.merge(weather_df,on=["creation_time"])
                final_df.dropna(inplace=True)
                if c_matrix:
                    correlation_matrix(final_df)
                
                print(f"null values in final_df: {final_df.isna().sum().sum()}")
                final_df.dropna(inplace=True)
                # final_df.reset_index(drop=True, inplace=True)
                final_df.set_index("creation_time",inplace=True)
                # final_df.drop(['creation_time'],axis=1,inplace=True)
                # final_df
                return final_df

        except Exception as e:
            logger.error(f"Error transforming data: {e}", exc_info=True)
            return pd.DataFrame()
