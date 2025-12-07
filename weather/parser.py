import pandas as pd

class WeatherParser:
    @staticmethod
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

        # Flatten nested 'weather' array (take first element)
        hourly_weather = pd.json_normalize(data["hourly"], record_path=["weather"], meta=["dt"])
        hourly_weather = hourly_weather.groupby("dt").first().reset_index()
        hourly_df = hourly_df.merge(hourly_weather, on="dt", how="left")

        # Drop original weather column
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

        # prepare for db insert
        cleaned_df = cleaned_df.rename(columns={
            "dt": "datetime",
            "id": "weather_id",
            "main": "weather_main",
            "description": "weather_description",
            "icon": "weather_icon"
        })

        return cleaned_df

    @staticmethod
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

        # Flatten nested 'weather' array (take first element)
        daily_weather = pd.json_normalize(data["daily"], record_path=["weather"], meta=["dt"])
        daily_weather = daily_weather.groupby("dt").first().reset_index()
        daily_df = daily_df.merge(daily_weather, on="dt", how="left")

        # Drop original weather column
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

        cleaned_df = cleaned_df.rename(columns={
            "dt": "date",
            "id": "weather_id",
            "main": "weather_main",
            "description": "weather_description",
            "icon": "weather_icon"
        })

        return cleaned_df

    @staticmethod
    def parse_hist(data, place_id):
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
        df['place_id'] = place_id

        # celsius to farenheight
        cleaned_df = df.copy()
        cleaned_df['temp'] = cleaned_df['temp'] * 9/5 + 32
        cleaned_df['feels_like'] = cleaned_df['feels_like'] * 9/5 + 32

        # Convert UNIX timestamps -> local datetime using timezone_offset 
        offset = cleaned_df['timezone_offset'].iloc[0]  # e.g. -21600 seconds = UTC-6 hours
        for col in ['dt', 'sunrise', 'sunset']:
            cleaned_df[col] = pd.to_datetime(cleaned_df[col] + offset, unit='s')

        cleaned_df = cleaned_df.rename(columns={
            "dt": "datetime",
            "id": "weather_id",
            "main": "weather_main",
            "description": "weather_description",
            "icon": "weather_icon"
        })

        return cleaned_df