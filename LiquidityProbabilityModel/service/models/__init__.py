import logging

import pandas
from eli5 import explain_prediction_df
from sklearn.externals import joblib
import numpy as np

from feature_builder import FeatureBuilder, PolandSellFeatureBuilder, \
    PolandRentFeatureBuilder,  PortugalFeatureBuilder
import features

logger = logging.getLogger()


def know(market, section):
    return '{0}_{1}'.format(market, section) in ALL

def get(market, section):
    return ALL['{0}_{1}'.format(market, section)]


class LiquidityEstimator(object):
    VERSION = '0.2'
    MODEL_FILE = ''
    COLUMNS_FILE = ''
    CITY_PRICES_FILE = ''
    FEATURES_BUILDER = object

    def __init__(self):
        print('Loading ML model {0}'.format(self.name()))

        self.model = joblib.load('data/' + self.MODEL_FILE)
        self.columns = joblib.load('data/' + self.COLUMNS_FILE)
        self.df_avg_city_prices = pandas.read_csv('data/' + self.CITY_PRICES_FILE)
        self.features_builder = self.FEATURES_BUILDER()

    def predict(self, request):
        df = self.to_features(request)
        prediction = self.model.predict_proba(df)
        return str(np.round(prediction[0][1], 3))

    def explain(self, request):
        df = self.to_features(request)
        explanation = explain_prediction_df(self.model, df.iloc[0], top=10)
        explanation['weight'] = explanation['weight'].apply(lambda x: np.round(x, 3))
        explanation['value'] = explanation['value'].apply(lambda x: np.round(x, 3))
        return explanation.to_csv(index=False).split('\n')

    def to_features(self, data):

        df = pandas.DataFrame.from_dict([dict(data)])
        # merge city avg prices
        df_aggregated = pandas.merge(df, self.df_avg_city_prices, on=['city_id', 'mysql_search_rooms_num'])
        df_aggregated.drop('city_id', inplace=True,  axis=1)

        # fill all known features
        fb = FeatureBuilder(df_aggregated)
        df = fb.featurize()

        # switch-off some features
        for trained_feature in self.columns:
            if trained_feature not in df.columns:
                df[trained_feature] = 0
        return df[self.columns]

    def name(self):
        return '{0}, version: {1}'.format(self.__class__, self.VERSION)


class PortugalRentEstimator(LiquidityEstimator):
    MODEL_FILE = 'xgb_imo_rent_02.pkl'
    COLUMNS_FILE = 'model_columns_xgb_imo_rent_02.pkl'
    CITY_PRICES_FILE = 'avg_city_price_imo_rent_02.csv'
    FEATURES_BUILDER = features.PortugalRentEncoder

    def to_features(self, data):
        df = data
        # merge city avg prices
        df_aggregated = pandas.merge(df, self.df_avg_city_prices, on=['city_id', 'rooms_num'])

        # fill all known features
        fb = PortugalFeatureBuilder(df_aggregated)
        df = fb.featurize()

        # switch-off some features
        for trained_feature in self.columns:
            if trained_feature not in df.columns:
                df[trained_feature] = 0
        return df[self.columns]


class PortugalSellEstimator(LiquidityEstimator):
    MODEL_FILE = 'xgb_imo_buy_02.pkl'
    COLUMNS_FILE = 'model_columns_xgb_imo_buy_02.pkl'
    CITY_PRICES_FILE = 'avg_city_price_imo_buy_02.csv'
    FEATURES_BUILDER = features.PortugalSellEncoder

    def to_features(self, data):
        df = data
        # merge city avg prices
        df_aggregated = pandas.merge(df, self.df_avg_city_prices, on=['city_id', 'rooms_num'])

        # fill all known features
        fb = PortugalFeatureBuilder(df_aggregated)
        df = fb.featurize()

        # switch-off some features
        for trained_feature in self.columns:
            if trained_feature not in df.columns:
                df[trained_feature] = 0
        return df[self.columns]



class PolandRentEstimator(LiquidityEstimator):
    MODEL_FILE = 'xgb_otodom_rent_02.pkl'
    COLUMNS_FILE = 'model_columns_xgb_otodom_rent_02.pkl'
    CITY_PRICES_FILE = 'avg_city_price_otodom_rent_02.csv'
    FEATURES_BUILDER = features.PolandRentEncoder

    def to_features(self, data):
        df = data
        # merge city avg prices
        df_aggregated = pandas.merge(df, self.df_avg_city_prices, on=['city_id', 'mysql_search_rooms_num'])
        # fill all known features
        fb = PolandRentFeatureBuilder(df_aggregated)
        df = fb.featurize()

        # switch-off some features
        for trained_feature in self.columns:
            if trained_feature not in df.columns:
                df[trained_feature] = 0
        return df[self.columns]



class PolandSellEstimator(LiquidityEstimator):
    MODEL_FILE = 'xgb_otodom_buy_02.pkl'
    COLUMNS_FILE = 'model_columns_xgb_otodom_buy_02.pkl'
    CITY_PRICES_FILE = 'avg_city_price_otodom_buy_02.csv'
    FEATURES_BUILDER = features.PolandSellEncoder

    def to_features(self, data):
        df = data
        # merge city avg prices
        df_aggregated = pandas.merge(df, self.df_avg_city_prices, on=['city_id', 'mysql_search_rooms_num'])

        # fill all known features
        fb = PolandSellFeatureBuilder(df_aggregated)
        df = fb.featurize()

        # switch-off some features
        for trained_feature in self.columns:
            if trained_feature not in df.columns:
                df[trained_feature] = 0
        return df[self.columns]


ALL = {
    'pt_rent': PortugalRentEstimator(),
    'pl_rent': PolandRentEstimator(),
    'pt_sell': PortugalSellEstimator(),
    'pl_sell': PolandSellEstimator(),
}
