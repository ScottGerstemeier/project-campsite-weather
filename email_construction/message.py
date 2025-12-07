import pandas as pd

class MessageBuilder:
    def __init__(self, queryer, db_name, schema, places_df):
        """
        queryer   : DataQueryer instance
        db_name   : Azure SQL database name
        schema    : schema name
        places_df : DataFrame with columns [id, place_name]
        """
        self.queryer = queryer
        self.db = db_name
        self.schema = schema
        self.places_df = places_df

    def _get_hourly(self, start, end):
        return self.queryer.query_dataframe(
            cols=[
                "place_id", "datetime", "temp", "humidity", "wind_speed",
                "weather_main", "weather_description"
            ],
            table_name=f"{self.db}.{self.schema}.hourly",
            filters=[
                f"datetime >= '{start}'",
                f"datetime <= '{end}'"
            ]
        )
    
    def _get_daily(self, start, end):
        return self.queryer.query_dataframe(
            cols=[
                "place_id", "date", "temp_day", "temp_night",
                "temp_min", "temp_max", "humidity",
                "wind_speed", "weather_main", "weather_description"
            ],
            table_name=f"{self.db}.{self.schema}.daily",
            filters=[
                f"date >= '{start.date()}'",
                f"date <= '{end.date()}'"
            ]
        )
    
    def _build_place_section(self, place_id, place_name, hourly_df, daily_df):
        text = f"=== {place_name} ===\n\n"

        # Hourly (if available)
        hw = hourly_df[hourly_df["place_id"] == place_id]
        if not hw.empty:
            text += "Hourly Forecast:\n"
            for _, r in hw.sort_values("datetime").iterrows():
                text += (
                    f"{r['datetime']:%Y-%m-%d %H:%M} — "
                    f"{r['weather_main']} ({r['weather_description']}), "
                    f"{r['temp']}°F, Humidity {r['humidity']}%, "
                    f"Wind {r['wind_speed']} mph\n"
                )
            text += "\n"

        # Daily (always available)
        dw = daily_df[daily_df["place_id"] == place_id]
        if not dw.empty:
            text += "Daily Forecast:\n"
            for _, r in dw.sort_values("date").iterrows():
                text += (
                    f"{r['date']:%Y-%m-%d} — "
                    f"{r['weather_main']} ({r['weather_description']}), "
                    f"High {r['temp_max']}°F, Low {r['temp_min']}°F, "
                    f"Wind {r['wind_speed']} mph\n"
                )
            text += "\n"

        return text
    
    def build_email(self, start, end):
        """Returns a full plain-text email body for all places."""
        hourly_df = self._get_hourly(start, end)
        daily_df = self._get_daily(start, end)

        email_body = "Weather Update\n"
        email_body += f"Camp Window: {start} → {end}\n\n"

        for _, p in self.places_df.iterrows():
            email_body += self._build_place_section(
                place_id=p["id"],
                place_name=p["place_name"],
                hourly_df=hourly_df,
                daily_df=daily_df
            )

        return email_body