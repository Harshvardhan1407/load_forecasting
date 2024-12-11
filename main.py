from fastapi import FastAPI, HTTPException, Depends, APIRouter,Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from pydantic import BaseModel
import requests
from passlib.context import CryptContext
from datetime import datetime, timedelta
import uvicorn
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from typing import Annotated , Optional, Any
from logger import logger
from pymongo import MongoClient
import pandas as pd
# from tensorflow.keras.models import load_model              # type: ignore
import pickle
import numpy as np
from scipy.spatial.distance import mahalanobis

import os
from dotenv import load_dotenv
load_dotenv()


sql_username = os.getenv("sql_username")
sql_password = os.getenv("sql_password")
api_host = os.getenv("api_host")
api_port = int(os.getenv("api_port"))

# print(f"sql_username :{sql_username}\nsql_password: {sql_password}\napi_host:{api_host}\napi_port:{api_port}\nmongo_host :{mongo_host}\nmongo_port:{mongo_port}\ndatabase: {m_db}\ncollection :{m_collection}")
app = FastAPI()

# Database setup
DATABASE_URL = f"mysql+mysqlconnector://{sql_username}:{sql_password}@localhost/users"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Create a router
router = APIRouter(prefix="/api/v1")
# Add the router to the FastAPI app
app.include_router(router)

@app.post("/")
def read_root():
    return {"message": "Hi Harsh!!! API server working fine.....!"}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # Module and FastAPI instance
        host=api_host,  # Host configuration
        port=api_port,       # Port configuration
        reload=True      # Auto-reload during development
    )


# Secret key for signing JWTs
SECRET_KEY = "your_secret_key"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


# Database model
class User(Base):
    __tablename__ = "users_login"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True)
    hashed_password = Column(String(255))
    login_count = Column(Integer, default=0)

# Create tables
Base.metadata.create_all(bind=engine)


# Dependency: Get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_mongodb_conncetion():
    try:
        mongo_host = os.getenv("mongo_host")
        mongo_port = os.getenv("mongo_port")
        m_db = os.getenv("database")
        m_collection = os.getenv("collection")
        client = MongoClient(f"mongodb://{mongo_host}:{mongo_port}/")
        mongo_db = client[m_db]
        mongo_collection = mongo_db[m_collection]
        logger.info(f"mongo connection established")
        return mongo_collection
    except Exception as e:
        logger.error(f"error in mongodb_connection {e}",exc_info= True)

# # In-memory user storage for simplicity (use a database in production)
# fake_users_db = {
#     "harsh": {
#         "username": "harsh_id",
#         "hashed_password": "$2b$12$5Nk9yqjzvfp0R.2O.uVbWumAA/m5pSKV2ShnZ4s.jBxtg7g120lgK",
#     }
# }

# Load your pre-trained ML model
MODEL_PATH = os.getenv("MODEL_PATH")
try:
    # loaded_model = load_model(MODEL_PATH)
    # model = joblib.load(MODEL_PATH)
    logger.info("Model loaded successfully")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise RuntimeError(f"Model loading error: {e}")

# Password hashing setup
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 password bearer token
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")


# Helper: Hash password
def get_password_hash(password):
    return pwd_context.hash(password)


# Helper: Verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


# Helper: Authenticate user
def authenticate_user(db,username: str, password: str):
    # user = fake_users_db.get(username)
    user = db.query(User).filter(User.username == username).first()

    if not user or not verify_password(password, user["hashed_password"]):
        return None
    return user


# Helper: Create access token
def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


# Token endpoint
@app.post("/login_token")
# def login(form_data: OAuth2PasswordRequestForm = Depends()):
def login(form_data: OAuth2PasswordRequestForm = Depends(), db=Depends(get_db)):

    # user = authenticate_user(form_data.username, form_data.password)
    user = authenticate_user(db, form_data.username, form_data.password)

    if not user:
        raise HTTPException(status_code=401, detail="Invalid username or password")
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


# # Protected route
# @app.post("/user_login")
# def read_users_me(token: str = Depends(oauth2_scheme), db=Depends(get_db)):
# # def read_users_me(token: str = Depends(oauth2_scheme)):
#     try:
#         payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
#         username = payload.get("sub")
#         if username is None:
#             raise HTTPException(status_code=401, detail="Invalid authentication token")
#     except JWTError:
#         raise HTTPException(status_code=401, detail="Invalid authentication token")
    
#     return {"username": username}


@app.post("/user_login")
def read_users_me(token: str = Depends(oauth2_scheme), db=Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication token")
        user = db.query(User).filter(User.username == username).first()
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication token")
    return {"username": user.username, "login_count": user.login_count}


@app.post("/create_user")
def create_user(username: str, password: str, db=Depends(get_db)):
    hashed_password = get_password_hash(password)
    user = User(username=username, hashed_password=hashed_password)
    db.add(user)
    db.commit()
    db.refresh(user)
    return {"message": "User created successfully", "username": user.username}


@app.put("/update_password")
def update_password(username: str, new_password: str, db=Depends(get_db)):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.hashed_password = get_password_hash(new_password)
    db.commit()
    return {"message": "Password updated successfully"}

@app.delete("/delete_user")
def delete_user(username: str, db=Depends(get_db)):
    user = db.query(User).filter(User.username == username).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    db.delete(user)
    db.commit()
    return {"message": "User deleted successfully"}

# Define a Pydantic model for the request body
class WeatherRequest(BaseModel):
    project: Optional[str] = None  # Default value
    from_date: datetime
    to_date: datetime
    site_id: Optional[str] = None

@app.post("/weather_data_ingestion")
def weather_data_ingestion_fn(weather_api_payload: WeatherRequest,mongo_collection=Depends(get_mongodb_conncetion)):
    try:
        project = weather_api_payload.project
        from_date = weather_api_payload.from_date.date()
        to_date = weather_api_payload.to_date.date()
        site_id = weather_api_payload.site_id
        logger.info(f"weather_api payload project:{project}\nsite_id:{site_id}\nfrom_date: {from_date}\n to_date: {to_date}")
        if project.lower() == "npcl":
            latitude, longitude = "28.46072","77.537381" #"28.625361","77.376214"#, 28.628059,77.378912
            site_id = "NPCL_id"

        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={from_date}&end_date={to_date}&hourly=relative_humidity_2m,apparent_temperature,rain,wind_speed_10m"
        response = requests.get(url)
        response.raise_for_status()
        weather_data = response.json()
        if weather_data:
            logger.info(f"Fetched {len(weather_data['hourly']['time'])} records from weather API")
            # logger.info(f"weather_data fetched :{len(weather_data)}")
            # return weather_data
            bulk_insert_data = []
            for i in range(len(weather_data['hourly']['time'])):
                hour_data = {
                    "_id": f"{site_id}_{weather_data['hourly']['time'][i]}",  # MongoDB's unique identifier
                    "time": weather_data['hourly']['time'][i],
                    "rain": weather_data['hourly'].get('rain', [])[i],
                    "relative_humidity_2m": weather_data['hourly'].get('relative_humidity_2m', [])[i],
                    "apparent_temperature": weather_data['hourly'].get('apparent_temperature', [])[i],
                    "wind_speed_10m": weather_data['hourly'].get('wind_speed_10m', [])[i],
                    }
                bulk_insert_data.append(hour_data)
            logger.info(f"bulk insert data: {len(bulk_insert_data)}")
            mongo_collection.insert_many(bulk_insert_data)
            logger.info(f"Stored weather data in DB from {from_date} to {to_date}")
        else:
            logger.warning("No data received from weather API")

        return {"message": "Weather data ingestion completed successfully"}
    except Exception as e:
        logger.error(f"Error in weather_data_ingestion_fn: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

@app.post("/fetch_weather_data")
def weather_data_fetching(weather_api_payload: WeatherRequest,mongo_collection=Depends(get_mongodb_conncetion)):
    try:
        project = weather_api_payload.project
        from_date = weather_api_payload.from_date.isoformat()
        to_date = weather_api_payload.to_date.isoformat()
        site_id = weather_api_payload.site_id
        # print(project,site_id)
        if project.lower() == "npcl":
            query_result = mongo_collection.find(
                {"time": {"$gte": from_date, "$lt": to_date}}
            )
        data = list(query_result)   # data  = [_ for _ in query_result]

        return {"message": "Weather data fetched successfully", "data count": len(data), "data": data}
    except Exception as e:
        logger.error(f"Error in weather data fetching: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {e}")

# Define a Pydantic model for the request body
class PredictionRequest(BaseModel):
    project: Optional[str] = None  # Default value
    dataset:  Optional[dict] = None
    site_id: Optional[str] = None
    from_date: datetime = None
    to_date: datetime = None
    
@app.post("/prediction")
def predition(prediction_api_payload: PredictionRequest):
    try:
        # feature_scaler = load_scalar()
        # print(feature_scaler)
        return {"message": "Prediction done"}
    except Exception as e:
        logger.error(f"Error in weather data fetching: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error: {e}")
    


# Load precomputed parameters
mean_vector = np.load('Models/anamoly_detection_parameters/mean_value.npy')
inv_cov_matrix = np.load('Models/anamoly_detection_parameters/inv_cov_matrix.npy')
threshold = np.load('Models/anamoly_detection_parameters/threshold.npy')

@app.post("/detect_anomaly")
async def detect_anomaly(data: dict):
    point = np.array([data['hourly'], data['daily']])
    distance = mahalanobis(point, mean_vector, inv_cov_matrix)
    is_anomaly = distance > threshold
    return {"mahalanobis_distance": distance, "is_anomaly": is_anomaly}