import asyncio

from motor.motor_asyncio import AsyncIOMotorClient

password = "Admin123"
uri = f"mongodb+srv://root:{password}@cluster0.xrumu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"


async def monitor():
    # 开启变更流（启用增量日志监控）
    client = AsyncIOMotorClient(uri)
    async with await client.start_session() as session:
        async with client.watch(full_document="updateLookup", session=session) as cursor:
            async for i in cursor:
                print(i)
                # todo
                # 用于数据库变化监控、增量备份、事件通知，类似于canal的日志收集模块


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor())
