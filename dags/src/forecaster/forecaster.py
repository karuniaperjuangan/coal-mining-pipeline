
import pandas as pd
from prophet import Prophet


class Forecaster:
    def __init__(self, df: pd.DataFrame, regressors: list[str] = None):
        self.df = df
        self.regressors = regressors
        self.model = None
        self.forecast = None

    def create_prophet_model(self):
        self.model = Prophet()
        # use external variable to improve model performance
        if self.regressors:
            for regressor in self.regressors:
                self.model.add_regressor(regressor)
        self.model.fit(self.df)

    def predict_future(self, periods: int):
        future = self.model.make_future_dataframe(periods=periods)
        if self.regressors:
            future[self.regressors] = self.df[self.regressors].ffill().iloc[-1][self.regressors].values[0]

        self.forecast = self.model.predict(future)
        return self.forecast
