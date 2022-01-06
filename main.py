from motor.motor_asyncio import AsyncIOMotorClient
import asyncio
from pymongo.results import InsertManyResult, InsertOneResult, UpdateResult, DeleteResult
from pymongo import MongoClient
from typing import AsyncIterable
import json
import pymongo

password = "Admin123"
uri = f"mongodb+srv://root:{password}@cluster0.xrumu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"


# index
# mongodb 中创建索引时使用传统mongo shell创建时db.collection.createIndex({"sample_filed": 1})
# 中的1表示递增索引
# -1表示递减索引

# data modeling
# rule：
# Data is stored in the way that it is used

# aggregate
# pipeline顺序在聚合操作中非常重要
# db = client[""]
#
# collection = db[""]
def sync_test():
    client = MongoClient(uri)
    db = client["sample_training"]
    for i in dir(db["trips"].find({
        # ne运算符仅支持单参数，如果需要传入多参数则需要使用nin运算符
        "birth year": {"$nin": ["", None]}
    }, {
        "birth year": True
    }).limit(1).sort([("birth year", pymongo.DESCENDING), ])):
        print(i)


async def test():
    # AsyncIOMotorClient、pymongo.MongoClient都内置了链接池
    client = AsyncIOMotorClient(uri)
    db = client["sample_training"]
    # client类似于执行一个mongo命令
    # result: AsyncIterable = await client.list_databases()
    # async for i in result:
    #     print(i)
    """
    {'name': 'sample_airbnb', 'sizeOnDisk': 54390784.0, 'empty': False}
    {'name': 'sample_analytics', 'sizeOnDisk': 9986048.0, 'empty': False}
    {'name': 'sample_geospatial', 'sizeOnDisk': 1179648.0, 'empty': False}
    {'name': 'sample_mflix', 'sizeOnDisk': 49029120.0, 'empty': False}
    {'name': 'sample_restaurants', 'sizeOnDisk': 6279168.0, 'empty': False}
    {'name': 'sample_supplies', 'sizeOnDisk': 1146880.0, 'empty': False}
    {'name': 'sample_training', 'sizeOnDisk': 48271360.0, 'empty': False}
    {'name': 'sample_weatherdata', 'sizeOnDisk': 2764800.0, 'empty': False}
    {'name': 'admin', 'sizeOnDisk': 286720.0, 'empty': False}
    {'name': 'local', 'sizeOnDisk': 1463431168.0, 'empty': False}
    """
    # result: list = await client.list_database_names()
    # print(result)

    # session用于操作具体document
    # mongodb不在意limit和sort的键入顺序，
    # mongodb会默认在limit前执行sort
    # mongodb假设每次limit之前都必须执行sort
    # null值在mongodb中进行排序时将作为最小值
    # 故进行排序之前一般都需要过滤掉null值
    async with await client.start_session() as session:
        db = client["sample_training"]

        # cursor = db["companies"].find({
        #     "founded_year": {"$ne": None}
        # }, {
        #    "name": 1, "founded_year": 1
        # }).limit(5).sort([("founded_year", pymongo.ASCENDING)])
        # async for i in cursor:
        #     print(i)

        cursor = db["trips"].find({
            # ne运算符仅支持单参数，如果需要传入多参数则需要使用nin运算符
            # "birth year": {"$nin": ["", None]}
        }, {
            "start station location.type": True
        }).limit(1)
        async for i in cursor:
            print(i)
        await cursor.close()
        # async with db["listingsAndReviews"].aggregate()
        #
        # result = await db["inspections"].count_documents(
        #     {
        #         "address.city": "NEW YORK"
        #     }
        # )
        # print(result)
    # cursor = db["listingsAndReviews"].aggregate(
    #         [{
    #             "$group": {
    #                 "_id": "$room_type",
    #             }
    #         }]
    # )
    # async for i in cursor:
    #     print(i)
    # await cursor.close()
    # result = await db["listingsAndReviews"].count_documents(
    #     {
    #         "property_type": "House",
    #         "amenities": "Changing table"
    #     }
    # )
    # print(result)

    # result = await db["listingsAndReviews"].count_documents(
    #     json.loads("""{
    #         "amenities": "Free parking on premises",
    #         "amenities": "Air conditioning",
    #         "amenities": "Wifi"
    #     }""")
    # )
    # print(result)
    # d = json.loads("""{
    #         "amenities": "Free parking on premises",
    #         "amenities": "Air conditioning",
    #         "amenities": "Wifi"
    #     }""")
    # print(d)
    # # 多个相同key的字典在python中仅有最后一个key的value会绑定这个key
    # # 在JSON中也相同，仅有最后一个key的value会绑定这个key
    # result = await db["listingsAndReviews"].count_documents(
    #     {
    #         "amenities": "Wifi"
    #     }
    # )
    # print(result)
    # result = await db["listingsAndReviews"].count_documents(
    #     {
    #         "amenities": {
    #             "$all": ["Free parking on premises", "Air conditioning", "Wifi"]
    #         }
    #     }
    # )
    # print(result)

    # projection 投影
    # 1.在find、count_documents方法中，投影的参数名为projection（pymongo >= 2.9）或fields（pymongo < 2.9）
    # 2.在find、count_documents方法中，投影可以作为位置参数传递，参数位置在filter后。如：collection.find({"a": 1}, {"_id": True})
    # 3.projection中field的是否展示选项仅支持同时为True或同时为False，除非特别指定排除_id field（_id默认展示选项为True），如：
    # collection.find({"a": 1}, {"name": True, "address": False})  # incorrect
    # collection.find({"a": 1}, {"name": True, "address": True})  # correct
    # collection.find({"a": 1}, {"name": True, "_id": False})  # correct
    # 4.projection可以自定义字段，就像SQL中子查询结果起别名放入查询结果，比如elemMatch运算符就可以完成这个功能
    # 5.elemMatch运算符匹配数组字段中至少一个对象满足要求
    # result = await db["companies"].count_documents({"offices": {"$elemMatch": {"city": "Seattle"}}})
    # print(result)

    #     # db = client["test_db"]
    #     result = await db["companies"].count_documents(
    #         {
    #             "$or": [
    #                 {
    #                     "founded_year": 2004,
    #                     "$expr": {
    #                         "category_code": {
    #                             "$or": ["social", "web"]
    #                         }
    #                     }
    #                 },
    #                 {
    #                     "founded_month": 10,
    #                     "$expr": {
    #                         "category_code": {
    #                             "$or": ["social", "web"]
    #                         }
    #                     }
    #                 }
    #             ]
    #         }
    #     )
    #     print(result)
    #
    #     result = await db["companies"].count_documents(
    #         {
    #             "$or": [
    #                 {
    #                     "founded_year": 2004,
    #                     "$expr": {
    #                         "$or": ["$category_code", "social", "web"]
    #                     }
    #                 },
    #                 {
    #                     "founded_month": 10,
    #                     "$expr": {
    #                         "$or": ["$category_code", "social", "web"]
    #                     }
    #                 }
    #             ]
    #         }
    #     )
    #     print(result)
    # 上述两个查询
    # 目的是查询companies collection中
    # (
    # founded_year == 2004
    # and (category == social or category == web)
    # ) or  (
    # founded_month == 10)
    # and (category == social or category == web)
    # 上面的第一个MQL是错误的
    # category_code在MQL里作为$expr的一个变量，而不是companies的一个字段
    # 即$expr运算符未起过滤作用
    # 上面的第二个MQL也是错误的
    # $or运算符的参数表示只要其中一个的bool运算值为true，就返回true。而social和web的bool运算值都是true
    # 即$expr运算符未起过滤作用
    # 正确的写法：
    # result = await db["companies"].count_documents(
    #     {
    #         "$or": [{
    #             "founded_year": 2004,
    #             "$or": [{"category_code": "social"}, {"category_code": "web"}]
    #         }, {
    #             "founded_month": 10,
    #             "$or": [{"category_code": "social"}, {"category_code": "web"}]
    #         }]
    #     }
    # )
    # print(result)
    #
    # result = await db["companies"].count_documents(
    #     {
    #         "$expr": {
    #             "$eq": ["$permalink", "$twitter_username"]
    #         }
    #     }
    # )
    # print(result)

    # test_db = client["test_db"]
    # result: InsertManyResult = await test_db["test_c"].insert_many([{"item_id": i} for i in range(50)],
    #                                                                session=session)
    # print(len(result.inserted_ids))

    # 增（insert_many）：
    # result: InsertManyResult = await db["test"].insert_many([{"item_id": i} for i in range(50)], session=session)
    # print(len(result.inserted_ids))

    # 增（insert_one）
    # result: InsertOneResult = await db["test"].insert_one({"item_id": 123, "hello": "world"}, session=session)
    # print(type(result))
    # print(result)
    # print(result.inserted_id)

    # 改（update_many）：
    # result: UpdateResult = await db["test"].update_many({"item_id": {"$gt": 10}}, {"$set": {"hello": "world"}}, session=session)
    # print(type(result))
    # print(result.modified_count)

    # 改（update_one）：
    # result: UpdateResult = await db["test"].update_one({"item_id": 123}, {"$set": {"hello": "world_exchanged"}}, session=session)
    # print(type(result))
    # print(result.modified_count)

    # 替换（replace_one）：
    # result: UpdateResult = await db["test"].replace_one({"item_id": 123}, {"hello": "world"}, session=session)
    # print(type(result))
    # print(result.modified_count)

    # 替换（replace_many）：
    # result: UpdateResult = await db["test"].replace_one({"item_id": {"$gt": 10, "$lt": 50}}, {"hello": "world"}, session=session)
    # print(type(result))
    # print(result.modified_count)

    # 查：sort、limit、find
    # result = db["companies"].find({"number_of_employees": {"$lt": 200}}, session=session).sort(
    #
    #     [("number_of_employees", pymongo.DESCENDING), ]).limit(1)
    # async for i in result:
    #     print(type(i))
    #     print(i)
    # result = await db["companies"].count_documents({"number_of_employees": {"$lt": 200}}, session=session)
    # print(result)

    # 删：
    # 删document：

    # delete_one

    # result: DeleteResult = await db["test"].delete_one({"item_id": 123}, session=session)
    # print(type(result))
    # print(result.deleted_count)
    # print(result.raw_result)

    # delete_many

    # result: DeleteResult = await db["test"].delete_many({"item_id": {"$gt": 10, "$lt": 50}}, session=session)
    # print(type(result))
    # print(result.deleted_count)
    # print(result.raw_result)

    # 删collection
    # result: Dict = await db.drop_collection("test", session=session)
    # print(result)
    # 成功
    # {'nIndexesWas': 1, 'ns': 'sample_training.test', 'ok': 1.0, '$clusterTime': {'clusterTime': Timestamp(1635235502, 1), 'signature': {'hash': b'\xdd\xed;|U\xfe`\x02\x1b`\xa7\xe9:\x1be\x92\xc8\xe8ec', 'keyId': 7020066454095527941}}, 'operationTime': Timestamp(1635235502, 1)}
    # 失败
    # {'operationTime': Timestamp(1635235612, 1), 'ok': 0.0, 'errmsg': 'ns not found', 'code': 26, 'codeName': 'NamespaceNotFound', '$clusterTime': {'clusterTime': Timestamp(1635235612, 1), 'signature': {'hash': b"\xa2\xb4Q\xb4\\\xe9'$\xfa\x01\xf7\n`\x90\xfe\x86\x9fZt\xb4", 'keyId': 7020066454095527941}}}

    # 删database
    # await client.drop_database("test_db", session=session)

    # 获取数据量
    # result = await db["test"].count_documents({}, session=session)
    # print(result)
    # result: AsyncIterable = test_db["test_c"].find(session=session)
    # async for i in result:
    #     print(i)
    # 开启变更流（启用增量日志监控）
    # async with client.watch(full_document="updateLookup", session=session) as cursor:
    #     async for i in cursor:
    #         print(i)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test())
    # print("*" * 100)
    # sync_test()
