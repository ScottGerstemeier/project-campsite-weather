def main():
    from config import API_KEY, DB_CONFIG, SMTP_CONFIG
    import pandas as pd
    from db.connection import AzureSQLDatabase
    from db.inserter import DataInserter
    from db.queryer import DataQueryer
    from weather.fetcher import WeatherFetcher
    from weather.parser import WeatherParser
    from email_construction.message import MessageBuilder
    from email_construction.sender import EmailSender

    # DB connection
    conn = AzureSQLDatabase(**DB_CONFIG).connect()
    inserter = DataInserter(conn)
    queryer = DataQueryer(conn)

    def fetch_and_insert_weather(conn, fetcher, parser, inserter, places_df, db_name, schema):
        for _, row in places_df.iterrows():
            place_id, lat, lon, tz = row["id"], row["latitude"], row["longitude"], row["timezone_name"]
            print(f"Processing {place_id}: lat={lat}, lon={lon}, tz={tz}")

            # One Call
            one_call_json = fetcher.get_one_call(lat, lon)
            hourly_df = parser.parse_hourly(one_call_json, place_id)
            daily_df  = parser.parse_daily(one_call_json, place_id)
            inserter.insert_dataframe(hourly_df, f"{db_name}.{schema}.hourly")
            inserter.insert_dataframe(daily_df,  f"{db_name}.{schema}.daily")

            # Yesterday history
            for hour in range(24):
                hist_json = fetcher.get_yesterday(lat, lon, tz, hour)
                hist_df   = parser.parse_hist(hist_json, place_id)
                inserter.insert_dataframe(hist_df, f"{db_name}.{schema}.yesterday")

    def send_emails(sender, builder, people_df, camp_dates_df):
        start = camp_dates_df["camp_start_datetime"].iloc[0]
        end   = camp_dates_df["camp_end_datetime"].iloc[0]
        email_body = builder.build_email(start, end)
        subject = "Camp Weather Update"

        for _, person in people_df.iterrows():
            to_email = person["email"]
            first_name = person.get("first_name", "")
            body = f"Hi {first_name},\n\n{email_body}"

            try:
                sender.send(to_email, subject, body)
                print(f"Email sent to {to_email}")
            except Exception as e:
                print(f"Failed to send to {to_email}: {e}")

    # Weather fetch/parse classes
    fetcher = WeatherFetcher(API_KEY)
    parser = WeatherParser()
    places_df = queryer.query_dataframe(
        cols=["id", "place_name", "latitude", "longitude", "timezone_name"]
        ,table_name=f"{DB_CONFIG['database']}.{DB_CONFIG['schema']}.places"
        ,filters=["is_include = 1"]
    )
    fetch_and_insert_weather(conn, fetcher, parser, inserter, places_df, DB_CONFIG['database'], DB_CONFIG['schema'])

    # Emails
    people_df = queryer.query_dataframe(
        cols=["email", "first_name", "last_name"]
        ,table_name=f"{DB_CONFIG['database']}.{DB_CONFIG['schema']}.emails"
        ,filters=["is_include = 1"]
    )
    camp_dates_df = queryer.query_dataframe(
        cols=["camp_start_datetime", "camp_end_datetime"]
        ,table_name=f"{DB_CONFIG['database']}.{DB_CONFIG['schema']}.camp_dates"
    )

    builder = MessageBuilder(queryer=queryer, db_name=DB_CONFIG['database'], schema=DB_CONFIG['schema'], places_df=places_df)
    sender = EmailSender(**SMTP_CONFIG)

    send_emails(sender, builder, people_df, camp_dates_df)

if __name__ == "__main__":
    main()