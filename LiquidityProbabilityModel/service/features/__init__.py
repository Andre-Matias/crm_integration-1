import pandas

def required(func):
    def func_wrapper(self, data):
        func_name = str(func).split('.')[1].split(' ')[0]
        if func_name not in data:
            raise Exception("'{0}' is mandatory parameter".format(func_name))
        return func(self, data)
    return func_wrapper

class FeaturesEncoder(object):
    def create(self, data):
        return {}

    @required
    def city_id(self, data):
        return int(data['city_id'])

    @required
    def rooms_number(self, data):
        return int(data['rooms_number'])


class PortugalRentEncoder(FeaturesEncoder):
    def create(self, data):
        return pandas.DataFrame.from_dict([dict(data)])

class PortugalSellEncoder(FeaturesEncoder):
    def create(self, data):
        return pandas.DataFrame.from_dict([dict(data)])


class PolandRentEncoder(FeaturesEncoder):
    def create(self, data):
        return pandas.DataFrame.from_dict([dict(data)])


class PolandSellEncoder(FeaturesEncoder):
    def create(self, data):
        return pandas.DataFrame.from_dict([dict(data)])




