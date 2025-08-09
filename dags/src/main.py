import extractor.fetch_csv
import extractor.fetch_mysql
import extractor.fetch_weather_api
import extractor.fetch_csv
import forecaster.predict_production
import dotenv

if __name__ == "__main__":
    # only used to test the function, actually run through dags
    dotenv.load_dotenv()
    extractor.fetch_mysql.fetch_mysql()
    extractor.fetch_weather_api.fetch_weather_api("2024-07-01","2025-06-30")
    extractor.fetch_csv.fetch_csv()
    forecaster.predict_production.forecast_production()

