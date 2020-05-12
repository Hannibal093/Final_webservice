from pymongo import MongoClient
import pandas as pd
Timestamp = pd.Timestamp
idx = pd.IndexSlice

class MongoConnection(object):

    def __init__(self):
        self.client = MongoClient('mongodb://hannibal:`1q@192.168.1.109:27017/news')

    def get_database(self, dbname):
        self.db = self.client[dbname]

    def get_collection(self, name):
        self.collection = self.db[name]


class NewsCollection(MongoConnection):

    def __init__(self):
        super(NewsCollection, self).__init__()
        self.get_database('news')
        self.get_collection('news.doc')

    def find_twenty(self, page):
        news_list = []

        if page >= 1:
            page -= 1
        else:
            page = 0
        if self.collection.find({}).count():
            for i in self.collection.find().skip(page*20).limit(20).sort('time',-1):
                news_list.append(i)
        return news_list

    def find_all(self):
        return self.collection.find({})

    def find_all_list(self):
        news_list = []
        news_data = self.collection.find({})
        for n in news_data:
            news_list.append(n)
        return news_list

    def find_single(self):
        return self.collection.find_one({})

    def find_single_for_stock(self, id):
        return self.collection.find_one({"_id": id}, {"_id": 0, "title": 1, "time": 1, "url": 1})

    def find_by_id(self, id):
        return self.collection.find_one({"_id": id})

    def find_by_source(self, source):
        news_list = []
        for i in self.collection.find({"source": source}):
            news_list.append(i)
        return news_list

    def find_collections(self):
        return self.db.list_collection_names()

    def count_doc(self):
        return self.collection.count_doc()

    def category_update(self, title, new_list):
        self.collection.find_one({"title": title}, {"$set": {"category": new_list}})

    # def update_and_save(self, obj):
    #    if self.collection.find({'id': obj.id}).count():
    #        self.collection.update({ "id": obj.id},{'id':123,'name':'test'})
    #    else:
    #        self.collection.insert_one({'id':123,'name':'test'})
    #
    # def remove(self, obj):
    #     if self.collection.find({'id': obj.id}).count():
    #        self.collection.delete_one({ "id": obj.id})


class StockCollection(MongoConnection):

    def __init__(self):
        super(StockCollection, self).__init__()
        self.get_database('stock')
        self.get_collection('stock.doc')

    def get_stock_list(self):
        stock_list = []
        stock_data = self.collection.find({}, {"_id": 0, "stock_id": 1, "stock_name": 1, 'industry':1}).sort("stock_id", 1)
        for i in stock_data:
            stock_list.append(i)
        return stock_list

    def get_stock_list_ftn(self, page):
        stock_list = []

        if page >= 1:
            page -= 1
        else:
            page = 0

        stock_data = self.collection.find({"industry":{"$exists":True},
                                           "price":{"$exists":True},
                                           "month":{"$exists":True},
                                           "bargin":{"$exists":True},
                                           "stock_name":{"$exists":True},},
                                          {"_id": 0, "stock_id": 1, "stock_name": 1, "industry":1}).sort("stock_id", 1).skip(page*15).limit(15)
        for i in stock_data:
            stock_list.append(i)
        return stock_list

    def get_single_by_id(self, stock_id='2330'):
        stock_data = self.collection.find_one({"stock_id": stock_id})
        return stock_data

    def get_by_name(self, stock_name='台積電'):
        stock_data = self.collection.find_one({"stock_name": stock_name})

        return stock_data

    def get_single_price(self, stock_id):
        stock_data = self.collection.find_one({"stock_id":stock_id},{"_id":0,"price":1})
        df = pd.DataFrame(stock_data['price']).set_index('date')
        return df

    def get_latest_price(self, stock_id):
        stock_data = self.collection.find_one({"stock_id":stock_id},{"_id":0,"price":1,"stock_name":1})
        df = pd.DataFrame(stock_data['price']).sort_values(by=['date'],ascending=True).tail(100).set_index('date')
        return df, stock_data['stock_name']

    def get_by_industry(self, industry, page=0):
        stock_list = []
        total_count = self.collection.count_documents({"industry":industry})
        l = max(15,total_count)
        if page * 15 <= total_count:
            pass
        else:
            if total_count % 15 == 0:
                page = (total_count // 15) - 1
            else:
                page = (total_count // 15)
        stock_data = self.collection.find({"industry":industry}, {"_id": 0, "stock_id": 1, "stock_name": 1, "industry":1})\
                                    .sort('stock_id', 1).limit(l).skip(page * 15)
        for s in stock_data:
            stock_list.append(s)
        return stock_list

    def get_stock_list_mi(self, industry='', stock_name='',stock_id='', page=0):
        stock_list = []
        if industry and stock_name:
            stock_data = self.collection.find({"industry": industry, "stock_name":stock_name},
                                              {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1})
        elif industry and stock_id:
            stock_data = self.collection.find({"industry": industry, "stock_id": stock_id},
                                              {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1})
        elif industry:
            stock_data = self.collection.find({"industry": industry},
                                              {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1})
        elif stock_name:
            stock_data = self.collection.find({"stock_name": stock_name},
                                              {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1})
        elif stock_id:
            stock_data = self.collection.find({"stock_id": stock_id},
                                              {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1})

    def get_relate_news(self, stock_id):
        try:
            stock_data = self.collection.find_one({"stock_id":stock_id}, {"relate_news":1})
            news_list = stock_data['relate_news']
        except:
            news_list = []
        return news_list

    def get_industry_exist(self):
        stock_data = self.collection.find({"industry": {"$exists": True}},
                                          {"_id": 0, "stock_id": 1, "stock_name": 1, "industry": 1}).sort("stock_id", 1)
        return stock_data

    def get_industry_exist2(self):
        stock_data = self.collection.find({"industry": {"$exists": True}}).sort("stock_id", 1)
        return stock_data

    def update_column(self, stock_id, dict_to_update):
        self.collection.update_one({"stock_id": stock_id}, {"$set": dict_to_update})

    def update_stock(self, stock_id, dict_to_update):
        self.collection.update_one({"stock_id": stock_id}, {"$set": dict_to_update}, upsert=True)


class UserCollection(MongoConnection):

    def __init__(self):
        super(UserCollection, self).__init__()
        self.get_database('user')
        self.get_collection('user.doc')

    def add_user(self, user_id, name, email):
        dict_to_update = {"id":user_id, 'name':name, 'email':email, 'stock':[]}
        self.collection.insert_one(dict_to_update)

    def get_subscribe(self, user_id):
        user_data = self.collection.find_one({"id": user_id})
        try:
            sub_stock = user_data['stock']
        except:
            sub_stock = []
        return sub_stock

    def subscribe_stock(self, user_id, stock_id):
        user_data = self.collection.find_one({"id": user_id})
        sub_stock = user_data['stock']
        sub_stock.append(stock_id)
        dict_to_update = {'stock': sub_stock}
        self.collection.update_one({'id': user_id}, {"$set": dict_to_update}, upsert=True)

    def unsubscribe_stock(self, user_id, stock_id):
        user_data = self.collection.find_one({"id": user_id})
        sub_stock = user_data['stock']
        sub_stock.remove(stock_id)
        dict_to_update = {'stock': sub_stock}
        self.collection.update_one({'id': user_id}, {"$set": dict_to_update}, upsert=True)