from MongodbHelper import MongodbHelper
from bson.objectid import ObjectId
import time

from utils import save2json

helper = MongodbHelper()
client = helper.login("账号", "密码")

print(helper.getDatabases())  # 查询所有已存在的数据库
print(helper.getTables('xxx'))  # 查询指定数据库

db_name = client.mlnx_data  # 需要单条查询的数据库名称
tb_name = db_name.device_collection  # 分页名称

# mongoDB_collection = helper.getRowCount(bdName, tablesName)  # 查询指定数据库中的指定分页
#
# for x in mongoDB_collection:
#     x['_id'] = time.strftime("%Y-%m-%d %H:%M:%S", x['_id'].generation_time.timetuple())  # 处理ObjectId
#     print(x)

# 查询最新的数据
rows = helper.getLatestOne(tb_name, 10)
for i in rows:
    print(i)

# 保存数据值本地json文件
# 注意，如果查询数据有某种类型的对象，请先将其转换为可dumps的数据类型再进行保存
save_path = r'/code/mongo4heartdata'
with open (save_path + '//testsave.txt', 'a') as f:
    f.write(json.dumps(rows))
    f.write("\n")

# save_path = r'D:\code\...'
# save2json(save_path, results)


# 指定查询
# print(helper.findOneById("5e799fd4ae0c2137e4d1509e"))
# print(helper.findElementsByColumn({'patientId': 1447}))

# 关闭连接
helper.logout()








