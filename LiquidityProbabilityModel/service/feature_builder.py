"""
author: Anastasiia Kornilova
email: anastasiia.kornilova@rebbix.com
desc:
"""

import pandas as pd



def filter_currency(dataframe, currency):
    dataframe = dataframe[dataframe['price[currency]'] == currency]
    return dataframe

def select_columns(dataframe, columns):
    dataframe = dataframe[columns]
    return dataframe

def filter_values(dataframe, col, values_list):
    dataframe = dataframe[dataframe[col].isin(values_list)]
    return dataframe

def remove_outliers(dataframe, col, lower_bound, upper_bound):
    dataframe = dataframe[(dataframe[col] >= lower_bound) & (dataframe[col] <= upper_bound)]
    return dataframe

def add_avg_price_city_ap_type(dataframe):
    city_n = pd.DataFrame({'city_n': dataframe.groupby(['city_id'])['ad_id'].nunique()}).reset_index()
    city_room_avg_price = pd.DataFrame({'avg_price': dataframe.groupby(['city_id', 'mysql_search_rooms_num'])['mysql_search_price'].mean()}).reset_index()
    dataframe = dataframe.merge(city_n, on='city_id', how='left')
    dataframe = dataframe.merge(city_room_avg_price, on = ['city_id', 'mysql_search_rooms_num'], how='left')
    #dataframe = dataframe[dataframe["city_n"] >= 30]
    return dataframe

def count_avg_price_city_ap_type(dataframe):
    city_n = pd.DataFrame({'city_n': dataframe.groupby(['city_id'])['ad_id'].nunique()}).reset_index()
    city_room_avg_price = pd.DataFrame({'avg_price': dataframe.groupby(['city_id', 'mysql_search_rooms_num'])['mysql_search_price'].mean()}).reset_index()
    return city_room_avg_price

def add_higher_city_room_avg(dataframe):
    dataframe['higher_city_room_mean'] = (dataframe['mysql_search_price'] > dataframe['avg_price']).astype('int')
    return dataframe

def private_business_binarization(dataframe):
    dataframe.private_business = dataframe.private_business.replace({'private': 1, 'business':0})
    dataframe.private_business = dataframe.private_business.fillna('no_info')
    return dataframe

def column_info(dataframe, col, new_column_name, drop=False):
    dataframe[new_column_name] = pd.notnull(dataframe[col]).astype('int')
    if drop:
        dataframe = dataframe.drop(col, axis=1)
    return dataframe


def create_construction_decade(dataframe):
    dataframe['construction_year_decade'] = dataframe['build_year'].fillna(0)\
    .apply(lambda year: year if (year > 1000 and year < 2030) else 'no_info')\
    .apply(lambda year: str(year)[0:3]+'0')
    dataframe = dataframe.drop('build_year', axis=1)
    return dataframe

def create_construction_decade_pt(dataframe):
    dataframe['construction_year_decade'] = dataframe['construction_year'].fillna(0) \
    .apply(lambda year: year if (year > 1000 and year < 2030) else 'no_info') \
    .apply(lambda year: str(year)[0:3] + '0')
    dataframe = dataframe.drop('construction_year', axis=1)
    return dataframe


def market_primary_binarization(dataframe):
    dataframe['market_primary'] = dataframe['market'].replace({'primary': 1, 'secondary':0})
    dataframe['market_primary'] = dataframe['market_primary'].astype('str')
    dataframe['market_primary'] = dataframe.market_primary.fillna('no_info')
    dataframe = dataframe.drop('market', axis=1)
    return dataframe

def create_avg_price_diff(dataframe, avg_price_col, price_col, new_column_name):
    dataframe[new_column_name] = (dataframe[price_col] - dataframe[avg_price_col])/dataframe[avg_price_col]
    return dataframe


def media_types_processing(dataframe):
    mt_test = dataframe.media_types.fillna("no_media")
    all_categories = []

    vals = mt_test.value_counts().index.tolist()

    for v in vals:
        possible_values = v.split("<->")
        for x in possible_values:
            if x not in all_categories:
                all_categories.append(x)

    categories = mt_test.apply(lambda x: x.split("<->"))

    for x in all_categories:
        dataframe[x] = categories.map(lambda current: x in current).astype('int')
    return dataframe


def equipment_types_processing(dataframe):
    mt_test = dataframe.equipment_types.fillna("no_equipment")
    all_categories = []

    vals = mt_test.value_counts().index.tolist()

    for v in vals:
        possible_values = v.split("<->")
        for x in possible_values:
            if x not in all_categories:
                all_categories.append(x)

    categories = mt_test.apply(lambda x: x.split("<->"))

    for x in all_categories:
        dataframe[x] = categories.map(lambda current: x in current).astype('int')
    return dataframe


def security_types_processing(dataframe):
    mt_test = dataframe.security_types.fillna("no_security")
    all_categories = []

    vals = mt_test.value_counts().index.tolist()

    for v in vals:
        possible_values = v.split("<->")
        for x in possible_values:
            if x not in all_categories:
                all_categories.append(x)

    categories = mt_test.apply(lambda x: x.split("<->"))

    for x in all_categories:
        dataframe[x] = categories.map(lambda current: x in current).astype('int')
    return dataframe


def extras_types_processing(dataframe):
    mt_test = dataframe.extras_types.fillna("no_extras")
    all_categories = []

    vals = mt_test.value_counts().index.tolist()

    for v in vals:
        possible_values = v.split("<->")
        for x in possible_values:
            if x not in all_categories:
                all_categories.append(x)

    categories = mt_test.apply(lambda x: x.split("<->"))

    for x in all_categories:
        dataframe[x] = categories.map(lambda current: x in current).astype('int')
    return dataframe


class FeatureBuilder(object):

    continuous_features = []
    categorical_features = []
    model_columns = []

    def __init__(self, dataframe):

        self.dataframe = dataframe

    def featurize(self):
        pass

    def feature_engineering(self, dataframe=None):

        for col in self.model_columns:
            if col not in dataframe.columns:
                #print(col)
                dataframe[col] = None
        return dataframe


class PolandSellFeatureBuilder(FeatureBuilder):

    continuous_features = ['title',
               'description',
               'mysql_search_m',
               'mysql_search_price',
               'building_floors_num',
               'n_images',
               'avg_price']

    categorical_features = ['market_primary',
                            'mysql_search_rooms_num',
                            'building_material',
                            'building_ownership',
                            'building_type',
                            'construction_status',
                            'floor_no',
                            'windows_type',
                            'heating']
    model_columns = [
                 'city_id',
                 'street_name',
                 'title',
                 'description',
                 'mysql_search_rooms_num',
                 'mysql_search_m',
                 'mysql_search_price',
                 'building_floors_num',
                 'building_material',
                 'building_ownership',
                 'building_type',
                 'construction_status',
                 'equipment_types',
                 'extras_types',
                 'floor_no',
                 'heating',
                 'market',
                 'media_types',
                 'private_business',
                 'build_year',
                 'security_types',
                 'windows_type',
                 'n_images',
                 'avg_price',
                 'higher_city_room_mean']


    def feature_engineering(self, dataframe=None):
        for col in self.model_columns:
            if col not in dataframe.columns:
                dataframe[col] = None

        dataframe = dataframe.pipe(add_higher_city_room_avg) \
            .pipe(private_business_binarization) \
            .pipe(column_info, col='build_year', new_column_name='construction_year_info') \
            .pipe(create_construction_decade) \
            .pipe(column_info, col='street_name', new_column_name='street_info', drop=True) \
            .pipe(media_types_processing) \
            .pipe(equipment_types_processing) \
            .pipe(security_types_processing) \
            .pipe(extras_types_processing) \
            .pipe(column_info, col='extras_types', new_column_name='extras_types_info', drop=True) \
            .pipe(column_info, col='media_types', new_column_name='media_types_info', drop=True) \
            .pipe(column_info, col='security_types', new_column_name='security_types_info', drop=True) \
            .pipe(column_info, col='equipment_types', new_column_name='equipment_types_info', drop=True) \
            .pipe(column_info, col='heating', new_column_name='heating_info', drop=False) \
            .pipe(market_primary_binarization) \
            .pipe(create_avg_price_diff, avg_price_col='avg_price', price_col='mysql_search_price',
                  new_column_name='price_diff')
        return dataframe

    def featurize(self):
        dataframe =  self.feature_engineering(self.dataframe)
        dataframe[self.continuous_features] = dataframe[self.continuous_features].fillna(0)
        dataframe[self.categorical_features] = dataframe[self.categorical_features].fillna('no_info').astype( 'str')
        dataframe = pd.get_dummies(dataframe)
        return dataframe

class PolandRentFeatureBuilder(FeatureBuilder):

    continuous_features = ['title',
                           'description',
                           'mysql_search_m',
                           'mysql_search_price',
                           'building_floors_num',
                           'n_images',
                           'avg_price']

    categorical_features = ['mysql_search_rooms_num',
                            'building_material',
                            'building_type',
                            'construction_status',
                            'floor_no',
                            'heating',
                            'windows_type',
                            'construction_year_decade',
                            'rent_to_students']


    model_columns = ['street_name',
                 'title',
                 'description',
                 'mysql_search_rooms_num',
                 'mysql_search_m',
                 'mysql_search_price',
                 'building_floors_num',
                 'building_material',
                 'building_type',
                 'construction_status',
                 'equipment_types',
                 'extras_types',
                 'floor_no',
                 'heating',
                 'rent_to_students',
                 'media_types',
                 'private_business',
                 'build_year',
                 'security_types',
                 'windows_type',
                 'n_images',
                 'avg_price',
                 'higher_city_room_mean']

    def feature_engineering(self, dataframe=None):
        for col in self.model_columns:
            if col not in dataframe.columns:
                # print(col)
                dataframe[col] = None

        dataframe = dataframe.pipe(add_higher_city_room_avg)\
            .pipe(private_business_binarization)\
            .pipe(column_info, col='build_year', new_column_name='construction_year_info')\
            .pipe(create_construction_decade)\
            .pipe(column_info, col='street_name', new_column_name='street_info', drop=True)\
            .pipe(media_types_processing)\
            .pipe(equipment_types_processing)\
            .pipe(security_types_processing)\
            .pipe(extras_types_processing)\
            .pipe(column_info, col='extras_types', new_column_name='extras_types_info', drop=True)\
            .pipe(column_info, col='media_types', new_column_name='media_types_info', drop=True)\
            .pipe(column_info, col='security_types', new_column_name='security_types_info', drop=True)\
            .pipe(column_info, col='equipment_types', new_column_name='equipment_types_info', drop=True)\
            .pipe(column_info, col='heating', new_column_name='heating_info', drop=False)\
            .pipe(create_avg_price_diff, avg_price_col='avg_price', price_col='mysql_search_price', new_column_name= 'price_diff')

        return dataframe

    def featurize(self):
        dataframe = self.feature_engineering(self.dataframe)
        dataframe[self.continuous_features] = dataframe[self.continuous_features].fillna(0)
        dataframe[self.categorical_features] = dataframe[self.categorical_features].fillna('no_info').astype('str')
        dataframe = pd.get_dummies(dataframe)
        return dataframe


class PortugalFeatureBuilder(FeatureBuilder):

    continuous_features = ['title',
               'description',
               'mysql_search_m',
               'mysql_search_price',
               'n_images2',
               'avg_price',
               'price_diff']

    categorical_features = ['energy_certificate',
               'rooms_num',
               'bathrooms_num',
               'condition',
               'energy_certificate',
               'negotiable',
               'exchange',
               'construction_year_decade']


    model_columns = ['city_id',
                 'title',
                 'description',
                 'private_business',
                 'n_images2',
                 'was_paid_for_post',
                 'is_paid_for_post',
                 'mysql_search_m',
                 'mysql_search_price',
                 'rooms_num',
                 'bathrooms_num',
                 'condition',
                 'energy_certificate',
                 'construction_year',
                 'negotiable',
                 'avg_price',
                 'exchange']

    def feature_engineering(self, dataframe=None):
        for col in self.model_columns:
            if col not in dataframe.columns:
                dataframe[col] = None

        dataframe = dataframe.pipe(add_higher_city_room_avg)\
            .pipe(private_business_binarization)\
            .pipe(column_info, col='construction_year', new_column_name='construction_year_info')\
            .pipe(create_construction_decade_pt)\
            .pipe(column_info, col='street_name', new_column_name='street_info', drop=True)\
            .pipe(column_info, col='energy_certificate', new_column_name='energy_certificate')\
            .pipe(column_info, col='bathrooms_num', new_column_name='bathrooms_info')\
            .pipe(column_info, col='condition', new_column_name='condition_info')\
            .pipe(create_avg_price_diff, avg_price_col='avg_price', price_col='mysql_search_price', new_column_name= 'price_diff')
        return dataframe

    def featurize(self):
        dataframe = self.feature_engineering(self.dataframe)
        dataframe[self.continuous_features] = dataframe[self.continuous_features].fillna(0)
        dataframe[self.categorical_features] = dataframe[self.categorical_features].fillna('no_info').astype('str')
        dataframe = pd.get_dummies(dataframe)
        return dataframe





