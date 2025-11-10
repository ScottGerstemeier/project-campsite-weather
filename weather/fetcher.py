import os
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

class WeatherFetcher:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def get_yesterday(self, lat, lon, local_tz_name, hour):
        local_tz = ZoneInfo(local_tz_name)
        local_yesterday = datetime.now(local_tz).date() - timedelta(days=1)
        local_dt = datetime(local_yesterday.year, local_yesterday.month, local_yesterday.day, hour, 0, 0, tzinfo=local_tz)
        utc_dt = local_dt.astimezone(timezone.utc)
        timestamp = int(utc_dt.timestamp())

        url = "https://api.openweathermap.org/data/3.0/onecall/timemachine"
        resp = requests.get(url, 
                            params={"lat": lat, 
                                    "lon": lon, 
                                    "dt": timestamp, 
                                    "units": "metric", 
                                    "appid": self.api_key
                                    }
                            )
        resp.raise_for_status()
        return resp.json()

    def get_one_call(self, lat, lon):
        url = "https://api.openweathermap.org/data/3.0/onecall"
        resp = requests.get(url, 
                            params={"lat": lat, 
                                    "lon": lon, 
                                    "exclude": "current,minutely,alerts", 
                                    "units": "metric", 
                                    "appid": self.api_key
                                    }
                            )
        resp.raise_for_status()
        return resp.json()