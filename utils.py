from bson.objectid import ObjectId

import json

class JSONEncoder(json.JSONEncoder):
    def default(self, o):   # pylint: disable=E0202
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)


def save2json(save_path, collection):
    results = list(collection)  # 将cursor转换为字典列表（会影响速度）
    with open(save_path + '\\testsave.json', 'w') as fout:
        json.dump(collection, fout)
    print("save complete")