try:
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    pd.set_option('display.max_columns', None)
    import os
    plt.rcParams["figure.figsize"]=14,5
    import holidays
    from datetime import timedelta, datetime
    import warnings
    warnings.filterwarnings("ignore")
    import yaml

    import seaborn as sns
    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
    import joblib
    # import tensorflow as tf
    # from tensorflow.keras.regularizers import l2                # type: ignore
    # from tensorflow.keras.optimizers import Adam                # type: ignore
    # from tensorflow.keras.callbacks import EarlyStopping        # type: ignore
    # from tensorflow.keras.models import Sequential              # type: ignore
    # from tensorflow.keras.layers import LSTM, Dense, Dropout    # type: ignore

    np.set_printoptions(precision=3, suppress=True)
    import pickle
    from math import sqrt
    from pymongo import MongoClient
    from logger import logger
except ImportError as e:
    print(f"error in importing library: {e}")


def get_mongodb_connection(purpose):
    try:
        mongo_host = os.getenv("mongo_host")
        mongo_port = os.getenv("mongo_port")

        if not mongo_host or not mongo_port:
            raise ValueError("MongoDB host or port not found in environment variables")

        purpose_mapping = {
            "ingestion": ("ingestion_database", "ingestion_collection"),
            "weather": ("weather_database", "weather_collection"),
            "prediction": ("prediction_database", "prediction_collection"),
        }

        if purpose.lower() not in purpose_mapping:
            raise ValueError(f"Invalid purpose '{purpose}'. Must be one of {list(purpose_mapping.keys())}")

        # Fetch corresponding database and collection names
        db_env, collection_env = purpose_mapping[purpose.lower()]
        m_db = os.getenv(db_env)
        m_collection = os.getenv(collection_env)
        # logger.info(f"mongo_db_database: {m_db}, collection: {m_collection}")
        if not m_db or not m_collection:
            raise ValueError(f"Database or collection not found for '{purpose}' purpose in environment variables")

        # Establish MongoDB connection
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        mongo_db = client[m_db]
        mongo_collection = mongo_db[m_collection]

        logger.info(f"Mongo connection established for {purpose} purpose")
        return mongo_collection

    except Exception as e:
        logger.error(f"Error in MongoDB connection: {e}", exc_info=True)


def load_schema(file_path):
    """Load schema from a YAML file."""
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def validate_dataframe_schema(df, schema):
    """Validate if DataFrame adheres to the schema."""
    expected_columns = schema.get("COLUMNS", {})
    
    for col, dtype in expected_columns.items():
        if col not in df.columns:
            raise ValueError(f"Missing column: {col}")
        if str(df[col].dtype) != dtype:
            raise ValueError(f"Column {col} has invalid type. Expected {dtype}, found {df[col].dtype}")

    logger.info("Schema validation passed")












def initial_data_ingestion():
    df_complete_data = pd.read_parquet('HT_meter_complete_data_9_id.parquet')
    df = df_complete_data[df_complete_data['sensor']=="6148740eea9db0.29702291"]  ## Jamia HT METER
    df = df.copy()

    df.drop("sensor", axis=1, inplace=True)
    print(f"df rows: {len(df)}")
    print(f"Duplicated rows: {df.duplicated().sum()}")
    df.set_index("creation_time", drop=True, inplace=True)
    df = df.loc[df.index.year>2022]
    df = df.loc[~ ((df.index.year==2024) & (df.index.month>5))]

    print(f"Duplicated rows after datetime index: {df.duplicated().sum()}")
    df = df.drop_duplicates()
    df = df[df['R_Voltage'] != 0]
    # df = df.loc[~df['R_Voltage'] == 0]
    print(f"Duplicated index: {df.index.duplicated().sum()}")
    print(f"null_values : {df.isna().sum().sum()}")
    print(f"df rows after basic checks: {len(df)}")
    return df

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
    hourly_data['dayofyear'] = hourly_data.index.dayofyear
    hourly_data['is_weekend'] = hourly_data['dayofweek'].isin([5, 6]).astype(int)
    hourly_data['holiday'] = 0
    # if lagre_data:
    hourly_data['month'] = hourly_data.index.month
    hourly_data['quarter'] = hourly_data.index.quarter
    hourly_data['weekofyear'] = hourly_data.index.isocalendar().week
    hourly_data['year'] = hourly_data.index.year

    return hourly_data

def plot_prediction(actual, predicted,timestamp=False):
    rmse = sqrt(mean_squared_error(actual, predicted))
    print("\nModel Power Evaluation")
    # print("Mean Squared Error (MSE):", mean_squared_error(actual, predicted))
    print("Mean Absolute Error (MAE):", mean_absolute_error(actual, predicted))
    print(f"Root Mean Squared Error (RMSE): {rmse}")
    print("R-squared:", r2_score(actual, predicted))

    # Scatter plot
    plt.figure(figsize=(10, 6))

    # Plot actual values in red
    if timestamp:
        plt.plot(actual.index, actual, color='red', label='Actual Power')
        # Plot predicted values in blue
        plt.plot(actual.index, predicted, color='blue', label='Predicted Power')    

    else:
        plt.plot(range(len(actual)), actual, color='red', label='Actual Power')
        # Plot predicted values in blue
        plt.plot(range(len(predicted)), predicted, color='blue', label='Predicted Power')
        # Adding labels and title

    plt.xlabel('Time')
    plt.ylabel('load')
    plt.title('Actual vs Predicted load')
    plt.legend()
    plt.tight_layout()
    plt.show()

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

def feature_label(dataset,target_variable = "KWh"):
    # train 
    dataset_features = dataset.copy()   
    dataset_label = dataset_features.pop(target_variable)
    print(f"dataset_features shape: {dataset_features.shape}\ndataset_label shape: {dataset_label.shape}")
    return dataset_features, dataset_label

def load_scalar():
    # with open('models/Min_Max_Scaler.pkl', 'rb') as f:
    with open("models/minmax_scaler_with_rolling.pkl", 'rb') as f:
        feature_scaler = pickle.load(f)
    return feature_scaler

def save_model( model,model_type="DL", model_dir="models", model_name="lstm_model"):
    # Ensure the directory exists

    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
    # Save the model in TensorFlow's SavedModel format
    model_path = os.path.join(model_dir, model_name)

    if model_type =="ML":
        ML_model_path = f"{model_path}.joblib"
        joblib.dump(model, ML_model_path)
    # return model_path
    else:
        model.save(model_path)  # SavedModel format
    
    print(f"Model saved to: {model_path}")
    # return model_path


def reshape_fn(dataset):
    return np.array(dataset).reshape(((dataset.shape[0]),1,dataset.shape[1]))

# def model_training(train_dataset,val_dataset,model):
    train_features,train_label = feature_label(train_dataset)
    val_features, val_label = feature_label(val_dataset)
    feature_scaler = load_scalar()
    
    if model == "ML":
        pass
    if model == "DL":
        reshaped_train_features= reshape_fn(feature_scaler.transform(train_features))
        reshaped_val_features = reshape_fn(feature_scaler.transform(val_features))
        print(reshaped_train_features.shape)
        print(reshaped_val_features.shape)
        
        # Build LSTM model with increased dropout and L2 regularization
        lstm_model = Sequential([            
            # Adding LSTM layers with increased dropout
            LSTM(units=64, return_sequences=True, input_shape=(reshaped_train_features.shape[1], reshaped_train_features.shape[2])),
            Dropout(0.3),  # Increased dropout
            LSTM(units=64, return_sequences=False),
            Dropout(0.3),  # Increased dropout
            # Adding dense output layer with L2 regularization
            Dense(units=1, kernel_regularizer=l2(0.01))  # L2 regularization
        ])
        # Compile the model
        lstm_model.compile(optimizer=Adam(learning_rate=0.001), loss='mean_squared_error'
        )
        # Define early stopping callback
        early_stopping = EarlyStopping(monitor='val_loss', patience=25, restore_best_weights=True)

        # Train the model with early stopping
        history = lstm_model.fit(reshaped_train_features, train_label, epochs=500, batch_size=64, 
                                validation_data=(reshaped_val_features, val_label), 
                                callbacks=[early_stopping])
        return lstm_model

def prediction(dataset,model):
    feature_scaler = load_scalar()
    dataset_features,dataset_label = feature_label(dataset)
    reshaped_dataset_features = reshape_fn(feature_scaler.transform(dataset_features))
    pred = model.predict(reshaped_dataset_features)
    plot_prediction(dataset_label, pred,timestamp =True)
    return pred 

def day_wise_prediction(dataset,day):
    return dataset.loc[dataset.index.day==day]

