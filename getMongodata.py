#!/usr/bin/python3

import pymongo
# 有密码连接https://zhuanlan.zhihu.com/p/51171906
# 注：以下授权连接方式为pymongo3.9之前版本的用法，新版本连接方式不同
mongo_client = pymongo.MongoClient('dds-uf63b7dddbaf8794-pub.mongodb.rds.aliyuncs.com', 3717)  # 数据库url，端口
mongo_auth = mongo_client.admin #或 mongo_client['admin'] admin为authenticationDatabase
mongo_auth.authenticate('root', 'Medgen2011NB!')  # 用户名，密码
print(mongo_client.server_info()) #判断是否连接成功


mongo_db = mongo_client['mlnx_data']  # 获取MongoDB中某个数据库
print(mongo_db)
mongo_collection = mongo_db['bp_collection']  # 获取某个数据库下的某个collection（相当于文件夹，可迭代）
for x in mongo_collection.find():
  print(x)
