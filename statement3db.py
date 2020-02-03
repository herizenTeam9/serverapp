from pymongo import MongoClient
url="mongodb://localhost:27017/"
client=MongoClient(url)
db=client.dhi_analytics
#print(client.list_database_names())
def get_academic_year():
    dhi_internal = db.dhi_internal
    academicyear=dhi_internal.aggregate([
        {"$group":{"_id":"null","academicyear":{"$addToSet":"$academicYear"}}},
        {"$project":{"academicyear":"$academicyear","_id":0}}
    ])
    for year in academicyear:
        year=year['academicyear']
    return year

def get_semesters():
    collection = db.dhi_student_attendance
    sems = collection.aggregate([
        {"$unwind":"$departments"},
        {"$group":{"_id":"null","sems":{"$addToSet":"$departments.termName"}}},
        {"$project":{"sems":1,"_id":0}}
        ])
    res = []
    for x in sems:
        res = x["sems"]
    #print(res)
    return res