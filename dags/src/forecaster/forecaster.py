
import pandas as pd
from prophet import Prophet


class Forecaster:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.model = None
        self.forecast = None

    def create_prophet_model(self):
        self.model = Prophet()
        self.model.fit(self.df)

    def predict_future(self, periods: int):
        future = self.model.make_future_dataframe(periods=periods)
        self.forecast = self.model.predict(future)
        return self.forecast
