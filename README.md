## 📣 简介

1.用dict快速创建SQL表，方便用Python做一些小程序时，对数据存储的烦劳

```python
db.insert({"user":"张三","password":"1234","age":66})
"""
上面语句将会自动创建数据表 t1(表名可以人工指定),并将这条记录保存到t1表中,建表结构如下
crate table t1(user text,password text,age  integer)
"""
```

2.支持将常见的Python数据结构保存到数据库，如list,tuple,Python对象，def定义的函数等

```python
insert_dict = {  # 举例支持的数据类型
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
```



3.可以快速将结构简单Excel表数据导入到数据库中，在一定程度上实现用SQL查询Excel

**【文字简介】**：dict_to_db 是一个便捷的将Python dict对象保存到关系型数据库(现支持sqlite)的工具,
让dict数据保存到SQLite数据库同使用MongoDB的便捷有一定的相似性,适合小项目，单机项目，小爬虫等快速开发使用，并提供了几个便捷将Excel文件导入数据库的方法，可以实现结构简单的Excel
用SQL查询Excel里面的数据，然后导出新的Excel文件，还可以注册函数，用函数处理Excel的数据后再导出数据，实现更丰富的功能

## ✨ 特性

* 简洁明了的API设计，函数名称和SQL语法一定程度统一，方便使用
* 增加更多方便快捷的数据插入函数
    * insert_or_update
    * insert_or_replace
    * excel_to_db 从Excel中导入数据到数据库
* 更多提升开发速度的小功能
    * 可以自动给插入数据添加插入时间，更新时间等
    * 可以自动 alter 合并表结构，方便便捷开发
    * 便捷的Excel处理函数，方便用SQL查询Excel文件并导出结果 (ps:结构过于复杂的Excel不适用)

## 🔰 安装

```shell
$  pip install dict_to_db
```

## 📝 使用

* 简单入门

```python
import datetime

from dict_to_db import DictToDb

db = DictToDb('demo.db') # 连接demo.db 数据库文件，没有则创建
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
db.insert(insert_dict)
result = db.select(table_name='t1')  # 默认的表名为t1,也可以自定义表名
print(result[0]['dict2'] == insert_dict['dict2'])  # True  原始数据与查询数据 保持一致
"""
上述代码自动创建的数据表结构如下：
CREATE TABLE t1 (
str text ,
int integer , 
float double , 
datetime timestamp ,
list json_text , 
dict json_text , 
obj obj 
)
"""
```

### 连接数据库

* 连接数据库文件

```python
db=DictToDb("demo.db") # 连接demo.db数据库
```



### 自定义表名

```python
db.insert({"username":"张三",'age':66},table_name="user")# 手动指定表名为user
```



### 创建表时 手动定义字段类型和字段描述信息

> 定义字段类型和字段描述信息，通过dict 里面key的名字来定义， 格式为【字段名@字段类型#字段描述信息】，具体详细使用如下

* 创建数据表并手动指定数据类型 【格式：字段名@字段类型】

```python
db.insert({"username":"张三",'age@text':66}) #手动指定age类型为text，如不指定，程序自动判断为integer
```

* 创建数据表并设置为主键【格式：字段名#字段描述信息（如是否是主键，default值，是否unique等）】

```python
db.insert({"username#pk":"张三",'age':66}) # 手动指定username字段为主键
```

* 创建数据表设置联合主键

```python
# 手动指定username和user_id为联合主键
db.insert({"username#pk":"张三","user_id#pk":"66",'age':66}) 
# 定义主键也可以使用全称 primary key  pk只是简写
db.insert({"username#primary key": "张三", "user_id#primary key": "66", 'age': 66})
```

* 创建数据表并给字段设置默认值【格式：字段名#default xxx】

```python
db.insert({"username":"张三",'age#default 0':66}) # 设置age的默认值为0
```

