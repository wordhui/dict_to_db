import datetime

from dict_to_db import DictToDb


def f1():
    db = DictToDb('demo.db')
    table_name = "t_demo"
    insert_dict = {
        'str': 'hello word',
        'int': 2,
        'float': 3.3,
        'datetime': datetime.datetime.now(),
        'list': [1, 2, 3],
        'dict': {"A": 1, "B": 2},
        'dict2': {1: '3', 4: '5'},
        'obj': object(),  # 对象是支持保存到数据库的
        # 'func':  # def 定义的函数是支持保存到数据库的，lambda 函数是不支持的
    }
    # 这里会自动创建table，并将数据保存到该table
    db.insert(insert_dict, table_name=table_name)
    result = db.select(table_name=table_name)
    print(result[0]['dict2'] == insert_dict['dict2'])


def f2():
    db = DictToDb("demo.db")
    # db.insert({"username#primary key": "张三", "user_id#primary key": "66", 'age': 66}, table_name='t2')
    db.insert({"username": "张三", 'age#default 0': 66}, table_name='t3')
    db.insert({"username": "李四"}, table_name='t3')


if __name__ == '__main__':
    f2()
