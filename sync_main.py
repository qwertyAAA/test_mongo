import pymongo

password = "Admin123"
uri = f"mongodb+srv://root:{password}@cluster0.xrumu.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
client = pymongo.MongoClient(uri)


db = client.get_database("sample_training")

result = db.command("db.grades.countDocuments({})")
print(result)
