import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import os


class MongodbHelper:
    """MongoDB助手

    方法:
        isServerRunning：mongodb服务器服务是否运行
        startServer：启动服务
        login：登录
        logout：登出，退出登录
        getDatabases：当前用户可见的库
        getTables：库中全部的表-集合
        getRowCount：表中全部行数量
        getAllRows：表中全部行
        findOneById：根据id查某行
        findElementsByColumn：根据k-v查符合条件的行
        insertOne：插入一行
        insertMulit：插入多行
        updateOnePswdByUser：有条件的更新一条记录
        updateMulitPswdByUser：使用正则或模糊查询进行更新
        deleteOneByPswd：有条件的删除记录
        deleteMulitByPswd：有条件的删除多条记录
        clearTable：清空表
        dropTable：删除表
        dropDb：删除数据库
    """

    instance = None
    instanced = False

    # 独一份内存空间，确保单例
    # 写法比较固定
    def __new__(cls):
        if cls.instance is None:
            cls.instance = super().__new__(cls)
        return cls.instance

    # 单例中仅一次初始化即可
    def __init__(self):
        if MongodbHelper.instanced:
            return

        MongodbHelper.instanced = True
        print("初始化地址：%s" % id(self))

    # # 判断mongo服务是否启动
    # def isServerRunning(self):
    #     logs = os.popen('ps -ef|grep mongo').readlines()  # 调用终端执行一行shell指令
    #     for line in logs:
    #         if 'mongod --dbpath=/Users/moonmen/appspace/mongodb-macos-x86_64-4.4.1/db --logpath=/Users/moonmen/appspace/mongodb-macos-x86_64-4.4.1/log/mongodb.log --port=27017 --logappend --fork --auth' in line:
    #             return True
    #     return False
    #
    # #启动服务
    # def startServer(self):
    #     cmd = 'mgdb_start.sh'
    #     logs = os.popen(cmd).readlines()
    #     for line in logs:
    #         if "process started successfully" in line:
    #             return True
    #     return False

    def login(self, userName, pswd):
        self.client = MongoClient(host='dds-uf63b7dddbaf8794-pub.mongodb.rds.aliyuncs.com', port=3717)
        # db = self.client.admin
        self.db = self.client['admin']
        self.db.authenticate(userName, pswd)
        return self.client

    def logout(self):
        self.client.close()

    # 库和表信息
    def getDatabases(self):
        return self.client.list_database_names()  # > show dbs

    def getTables(self, db_name):
        return self.__getSingleTables(db_name)

    def __getSingleTables(self, dbName):
        # db = self.client.db_test # 用点方法访问库
        db = self.client[dbName]  # 用列表引用方法访问库
        return db.list_collection_names()  # > show tables

    #查询
    def getRowCount(self, db_name, tb_name):
        '''
        :param db_name: 指定数据库名称
        :param tb_name: 指定的分页名称
        :return: 返回cursor迭代体
        '''
        # results = tb.find().sort([('key1',pymongo.ASCENDING),('key2', pymongo.ASCENDING)]) #多条件排序
        # results = tb_name.find().sort('key', pymongo.ASCENDING).skip(2) #分页提取数据
        # list(self.__getRowCount(db_name, tb_name))
        results = self.__getRowCount(db_name, tb_name)
        return results

    def __getRowCount(self, dbName, tbName):
        return self.client[dbName][tbName].find()

    def getAllRows(self, db_name, tb_name):
        return self.__getAll(db_name, tb_name)

    def __getAll(self, dbName, tbName):
        cursor = self.client[dbName][tbName].find()
        eles = []
        for ele in cursor:
            eles.append(ele)
        self.client.close_cursor(cursor.cursor_id)  # 关闭表的游标
        return eles

    def getLatestOne(self, tb, back_nums=1):
        '''
        :param tb: 指定的分页名称
        :param back_nums: 需要查询的数据条数
        :return: 返回cursor迭代体
        '''
        # db = self.client.mlnx_data  # 需要单条查询的数据库名称
        # tb = db.device_collection  # 分页名称
        cursor_rows = tb.find().sort('_id', -1).limit(back_nums)
        if back_nums == 1:
            return next(cursor_rows)
        elif back_nums > 1 and back_nums > 0:
            return cursor_rows  # 返回可迭代的数据
        # 使用节点管理器
        # with tb.find(no_cursor_timeout=True).sort('_id', -1).limit(back_nums) as cursor_rows:
        #     # for row in cursor_rows:
        #     #     parse_data(row)
        #     if back_nums == 1:
        #         return next(cursor_rows)
        #     elif back_nums > 1 and back_nums > 0:
        #         return cursor_rows  # 返回可迭代的数据

    def findOneById(self, _id):
        db = self.client.mlnx_data  # 需要单条查询的数据库名称
        tb = db.device_collection  # 分页名称
        return tb.find_one({'_id': ObjectId(_id)})


    def findElementsByColumn(self, col):
        db = self.client.mlnx_data
        tb = db.device_collection
        return tb.find_one(col)

    def blurryFind(self):
        """
            模糊查询

            符号含义示例
                $lt 小于{'age': {'$lt': 20}} age值小于20
                $gt 大于{'age': {'$gt': 20}}
                $lte 小于等于{'age': {'$lte': 20}}
                $gte 大于等于{'age': {'$gte': 20}}
                $ne 不等于{'age': {'$ne': 20}}
                $in 在范围内{'age': {'$in': [20, 23]}}
                $nin 不在范围内{'age': {'$nin': [20, 23]}}
        """
        pass

    def regexFind(self):
        '''
        正则查询

        符号含义示例
            $regex 匹配正则 {'name': {'$regex': '^M.*'}} name以M开头
            $exists 属性是否存在 {'name': {'$exists': True}} name属性存在
            $type 类型判断 {'age': {'$type': 'int'}} age的类型为int
            $mod 数字模操作 {'age': {'$mod': [5, 0]}} 年龄模5余0
            $where 高级条件查询 {'$where': 'obj.fans_count == obj.follows_count'} 自身粉丝数等于关注数
        '''
        pass
    # 增
    def insertOne(self):
        db = self.client.db_test
        tb = db.tb_test
        tb = db['tb_test']
        return tb.insert_one({"user": "小王", "pswd": "xiaowang123"})

    def insertMulit(self):
        datas = [{"user": "小王{}".format(i), "pswd": "xiaowang12{}".format(i)} for i in range(5)]
        tb = self.client['db_test']['tb_test']
        return tb.insert_many(datas)

    # 改
    def updateOnePswdByUser(self, u, new_pswd):
        condition = {"user": u}
        db = self.client.db_test
        tb = db.tb_test
        user = tb.find_one(condition)
        user['pswd'] = new_pswd
        # return tb.update_one(condition, user) # 更新符合条件的第一条记录。update参数只接受$开头的符号语句$set、$inc此类
        # return tb.find_one_and_update(condition, user) # find_one_and_update的update参数只接受$开头的符号语句$set、$inc此类
        return tb.find_one_and_replace(condition, user)

    def updateMulitPswdByUser(self, user):
        condition = {'user': {'$regex': '.*{}.*'.format(user)}}  # 模糊匹配 如果是空的{}则更新全部条目
        db = self.client.db_test
        tb = db.tb_test
        return tb.update_many(condition, {'$inc': {'age': 1}})  # age必须是int类型

    def dynamicUpdate(self):
        '''
            动态更新

            update修改器（update_one示例）:
                $inc：变量值1后保存
                    tb.update_one({"age":90}, {"$inc":{"age":1}}}) 将原先的age值90加1
                $set：没有指定项就自动添加，有则更新
                    tb.update_one({"user":"小明"}, {"$set":{"single":true}}}) 给”小明“这一行增加(或更新)一个字段single，值为true
                $unset：删除指定项
                    tb.update_one({"user":"小明"}, {"$unset":{"single":1}}}) 查找”小明“这一行，删除字段single
                $push：在列表属性(list)的项-字段尾端加入新元素
                    tb.update_one({"user":"小明"}, {"$push":{"like_list":"动画"}}}) 在like_list这个列表中插入一个‘动画’
                $pull：删除列表属性的项目中的指定元素-不限位置和数量
                    tb.update_one({"user":"小明"}, {"$pull":{"like_list":"动画"}}}) 将like_list这个列表中全部的‘动画’删除
                $pop：删除列表属性项目中的第一个（1）或最后一个（-1）元素
                    tb.update_one({"user":"小明"}, {"$pop":{"like_list":1}}}) 在like_list这个列表中第一个‘动画’移除
                $addToSet：向列表字段插入元素
                    tb.update({"user":"小明"},{"$addToSet":{"like_list":['电影', '绘画', '游泳']}}) 向like_list插入多个爱好
                $push、$addToSet参数$each、$slice、$sort（$slice"、"$sort"须配合"$each"使用）：
                    插入随机一个元素
                        tb.update_one({"user":"小明"},{"$addToSet":{"like_list":{"$each":['电影', '绘画', '游泳']}}}) 向like_list插入一个爱好
                    插入后保留末尾2个元素。$slice不能用在addToSet
                        tb.update_one({"user":"小明"},{"$push":{"like_list":{"$each":['电影', '绘画', '游泳'], "$slice":-2}}})
                    排序，插入，保留最后2个元素
                        tb.update_one({"user":"小明"},{"$push":{"like_list":{"$each":['电影', '绘画', '游泳'], "$slice":-2, "$sort":-1}}})
        '''

    # 删
    def deleteOneByPswd(self, pswd):
        db = self.client.db_test
        tb = db.tb_test
        # tb.delete_one({"pswd": pswd})
        tb.find_one_and_delete({"pswd": pswd})

    def deleteMulitByPswd(self, pswd):
        db = self.client.db_test
        tb = db.tb_test
        tb.delete_many({'pswd': {'$regex': '.*{}.*'.format(pswd)}})  # 如果参数传入'123'，删除密码里带有‘123’字样的

    def clearTable(self):
        db = self.client.db_test
        tb = db.tb_test
        tb.delete_many({})  # 条件为空

    def clearTable2(self):
        tb = self.client['db_test']['tb_test']
        tb.remove()  # tb.delete_many({})

    def dropTable(self):
        tb = self.client['db_test']['tb_test']
        tb.drop()  # 删除表

    def dropDb(self):
        self.client.drop_database('db_test')  # 删除数据库