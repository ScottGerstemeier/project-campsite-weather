from dotenv import load_dotenv
import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import pyodbc
import math

load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")

SERVER = 'localhost'
DATABASE = 'prod'
TRUSTED_CONNECTION = 'yes'

def get_yesterday(api_key, server, database, trusted_connection, place_id, hour):

    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection={trusted_connection};'
    )

    lat = pd.read_sql(f"SELECT latitude FROM prod.weather.places WHERE id = {place_id}", conn).iloc[0,0]
    lon = pd.read_sql(f"SELECT longitude FROM prod.weather.places WHERE id = {place_id}", conn).iloc[0,0]
    timezone_name = pd.read_sql(f"SELECT timezone_name FROM prod.weather.places WHERE id = {place_id}", conn).iloc[0,0]

    conn.close()

    # yesterday's date in that timezone
    local_tz = ZoneInfo(timezone_name)
    local_yesterday = datetime.now(local_tz).date() - timedelta(days=1)
    # create datetime at the given local hour
    local_dt = datetime(local_yesterday.year, local_yesterday.month, local_yesterday.day, hour, 0, 0, tzinfo=local_tz)
    # convert to UTC
    utc_dt = local_dt.astimezone(timezone.utc)
    yesterday_ts = int(utc_dt.timestamp())

    hist_url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
    hist_params = {
    "lat": lat,
    "lon": lon,
    "dt": yesterday_ts,
    "units": "metric",
    "appid": api_key
    }

    hist_resp = requests.get(hist_url, params=hist_params)
    hist_resp.raise_for_status()
    hist_data = hist_resp.json()

    return hist_data


def parse_hist_data(data):
    # unnest main JSON contents
    df = pd.json_normalize(
        data['data'],
        record_path='weather',
        meta=[
            'dt', 'sunrise', 'sunset', 'temp', 'feels_like', 'pressure', 
            'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 
            'wind_speed', 'wind_deg'
        ]
    )

    # Add metadata
    df['latitude'] = data['lat']
    df['longitude'] = data['lon']
    df['timezone'] = data['timezone']
    df['timezone_offset'] = data['timezone_offset']
    df['place_id'] = data

    # celsius to farenheight
    cleaned_df = df.copy()
    cleaned_df['temp'] = cleaned_df['temp'] * 9/5 + 32
    cleaned_df['feels_like'] = cleaned_df['feels_like'] * 9/5 + 32

    # Convert UNIX timestamps -> local datetime using timezone_offset 
    offset = cleaned_df['timezone_offset'].iloc[0]  # e.g. -21600 seconds = UTC-6 hours
    for col in ['dt', 'sunrise', 'sunset']:
        cleaned_df[col] = pd.to_datetime(cleaned_df[col] + offset, unit='s')

    return cleaned_df

def insert_yesterday(df, server, database, trusted_connection):

    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection={trusted_connection};'
    )
    cursor = conn.cursor()

    # --- Insert each record ---
    insert_query = """INSERT INTO prod.weather.yesterday (
        place_id
        ,latitude
        ,longitude
        ,timezone
        ,timezone_offset
        ,datetime
        ,sunrise
        ,sunset
        ,temp
        ,feels_like
        ,pressure
        ,humidity
        ,dew_point
        ,uvi
        ,clouds
        ,visibility
        ,wind_speed
        ,wind_deg
        ,weather_id
        ,weather_main
        ,weather_description
        ,weather_icon
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row['place_id'],
            row['latitude'],
            row['longitude'],
            row['timezone'],
            row['timezone_offset'],
            row['dt'],
            row['sunrise'],
            row['sunset'],
            row['temp'],
            row['feels_like'],
            row['pressure'],
            row['humidity'],
            row['dew_point'],
            row['uvi'],
            row['clouds'],
            row['visibility'],
            row['wind_speed'],
            row['wind_deg'],
            row['id'],
            row['main'],
            row['description'],
            row['icon']
        ))

    conn.commit()
    cursor.close()
    conn.close()

def get_one_call(api_key, server, database, trusted_connection, place_id):
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection={trusted_connection};'
    )

    lat = pd.read_sql(f"SELECT latitude FROM prod.weather.places WHERE id = {place_id}", conn).iloc[0,0]
    lon = pd.read_sql(f"SELECT longitude FROM prod.weather.places WHERE id = {place_id}", conn).iloc[0,0]

    conn.close()
    
    oc_url = "https://api.openweathermap.org/data/3.0/onecall"
    oc_params = {
    "lat": lat,
    "lon": lon,
    "appid": api_key,
    "exclude": "current,minutely,alerts",
    "units": "metric"
    }
    
    oc_resp = requests.get(oc_url, params=oc_params)
    oc_resp.raise_for_status()
    oc_data = oc_resp.json()

    return oc_data

def parse_hourly(data, place_id):
    meta_df = pd.DataFrame([{
        "place_id": place_id,
        "latitude": data["lat"],
        "longitude": data["lon"],
        "timezone": data["timezone"],
        "timezone_offset": data["timezone_offset"]
    }])

    hourly_df = pd.json_normalize(
        data["hourly"],
        sep="_"
    )

    # Flatten nested 'weather' array — take first element
    hourly_weather = pd.json_normalize(data["hourly"], record_path=["weather"], meta=["dt"])
    hourly_weather = hourly_weather.groupby("dt").first().reset_index()
    hourly_df = hourly_df.merge(hourly_weather, on="dt", how="left")

    # Drop original 'weather' column
    if "weather" in hourly_df.columns:
        hourly_df = hourly_df.drop(columns=["weather"])

    meta_expanded = pd.concat([meta_df]*len(hourly_df), ignore_index=True)
    hourly_df = pd.concat([meta_expanded, hourly_df.reset_index(drop=True)], axis=1)

    # celsius to farenheight
    cleaned_df = hourly_df.copy()
    cleaned_df['temp'] = cleaned_df['temp'] * 9/5 + 32
    cleaned_df['feels_like'] = cleaned_df['feels_like'] * 9/5 + 32

    # Convert UNIX timestamps -> local datetime using timezone_offset 
    offset = cleaned_df['timezone_offset'].iloc[0]  # e.g. -21600 seconds = UTC-6 hours
    cleaned_df["dt"] = pd.to_datetime(cleaned_df["dt"] + offset, unit='s')

    return cleaned_df

def insert_hourly(df, server, database, trusted_connection):
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection={trusted_connection};'
    )
    cursor = conn.cursor()

    # --- Insert each record ---
    insert_query = """INSERT INTO prod.weather.hourly (
        place_id
        ,latitude
        ,longitude
        ,timezone
        ,timezone_offset
        ,datetime
        ,temp
        ,feels_like
        ,pressure
        ,humidity
        ,dew_point
        ,uvi
        ,clouds
        ,visibility
        ,wind_speed
        ,wind_deg
        ,wind_gust
        ,pop
        ,rain_1h
        ,weather_id
        ,weather_main
        ,weather_description
        ,weather_icon
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        # Convert NaN to None for SQL NULL
        values = tuple(None if (isinstance(x, float) and math.isnan(x)) else x for x in [
            row['place_id'],
            row['latitude'],
            row['longitude'],
            row['timezone'],
            row['timezone_offset'],
            row['dt'],
            row['temp'],
            row['feels_like'],
            row['pressure'],
            row['humidity'],
            row['dew_point'],
            row['uvi'],
            row['clouds'],
            row['visibility'],
            row['wind_speed'],
            row['wind_deg'],
            row['wind_gust'],
            row['pop'],
            row.get('rain_1h'),  # this col can have missing values
            row['id'],
            row['main'],
            row['description'],
            row['icon']
        ])
        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()

def parse_daily(data, place_id):
    meta_df = pd.DataFrame([{
        "place_id": place_id,
        "latitude": data["lat"],
        "longitude": data["lon"],
        "timezone": data["timezone"],
        "timezone_offset": data["timezone_offset"]
    }])

    daily_df = pd.json_normalize(
        data["daily"],
        sep="_"
    )

    # Flatten nested 'weather' array — take first element
    daily_weather = pd.json_normalize(data["daily"], record_path=["weather"], meta=["dt"])
    daily_weather = daily_weather.groupby("dt").first().reset_index()
    daily_df = daily_df.merge(daily_weather, on="dt", how="left")

    # Drop original 'weather' column
    if "weather" in daily_df.columns:
        daily_df = daily_df.drop(columns=["weather"])

    meta_expanded = pd.concat([meta_df]*len(daily_df), ignore_index=True)
    daily_df = pd.concat([meta_expanded, daily_df.reset_index(drop=True)], axis=1)

    # celsius to farenheight
    cleaned_df = daily_df.copy()
    for col in ['temp_day', 
                'temp_min', 
                'temp_max', 
                'temp_night', 
                'temp_eve', 
                'temp_morn', 
                'feels_like_day',
                'feels_like_night',
                'feels_like_eve',
                'feels_like_morn']:
        cleaned_df[col] = cleaned_df[col] * 9/5 + 32

    # Convert UNIX timestamps -> local datetime using timezone_offset 
    offset = cleaned_df['timezone_offset'].iloc[0]  # e.g. -21600 seconds = UTC-6 hours
    for col in ['dt', 'sunrise', 'sunset', 'moonrise', 'moonset']:
        cleaned_df[col] = pd.to_datetime(cleaned_df[col] + offset, unit='s')

    return cleaned_df

def insert_daily(df, server, database, trusted_connection):
    conn = pyodbc.connect(
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'Trusted_Connection={trusted_connection};'
    )
    cursor = conn.cursor()

    # --- Insert each record ---
    insert_query = """INSERT INTO prod.weather.daily (
        place_id
        ,latitude
        ,longitude
        ,timezone
        ,timezone_offset
        ,[date]
        ,sunrise
        ,sunset
        ,moonrise
        ,moonset
        ,moon_phase
        ,summary
        ,pressure
        ,humidity
        ,dew_point
        ,wind_speed
        ,wind_deg
        ,wind_gust
        ,clouds
        ,pop
        ,rain
        ,uvi
        ,temp_day
        ,temp_min
        ,temp_max
        ,temp_night
        ,temp_eve
        ,temp_morn
        ,feels_like_day
        ,feels_like_night
        ,feels_like_eve
        ,feels_like_morn
        ,weather_id
        ,weather_main
        ,weather_description
        ,weather_icon
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        # Convert NaN to None for SQL NULL
        values = tuple(
            None if (isinstance(x, float) and math.isnan(x)) else x
            for x in [
                row['place_id'],
                row['latitude'],
                row['longitude'],
                row['timezone'],
                row['timezone_offset'],
                row['dt'],
                row['sunrise'],
                row['sunset'],
                row['moonrise'],
                row['moonset'],
                row['moon_phase'],
                row['summary'],
                row['pressure'],
                row['humidity'],
                row['dew_point'],
                row['wind_speed'],
                row['wind_deg'],
                row['wind_gust'],
                row['clouds'],
                row['pop'],
                row.get('rain'),
                row['uvi'],
                row['temp_day'],
                row['temp_min'],
                row['temp_max'],
                row['temp_night'],
                row['temp_eve'],
                row['temp_morn'],
                row['feels_like_day'],
                row['feels_like_night'],
                row['feels_like_eve'],
                row['feels_like_morn'],
                row['id'],
                row['main'],
                row['description'],
                row['icon']
            ])
        cursor.execute(insert_query, values)

    conn.commit()
    cursor.close()
    conn.close()