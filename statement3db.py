from pymongo import MongoClient
import pprint as pp
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

def get_student_usn(email):
    collection = db.dhi_user
    usn = collection.aggregate([
        {"$match":{"email":email}},
        {"$project":{"_id":0,"usn":1}}
    ])
    res = []
    for x in usn:
        res = x["usn"]
    #print(res)
    return res


def get_student_placment_offers(term, usn):
    collection=db.pms_placement_student_details
    offers = collection.aggregate([
        {"$unwind":"$studentList"},
        {"$match":{"studentList.regNo":usn,"academicYear":term}},
        {"$project":{"companyName":1,"salary":1,"_id":0}}
    ])
    res = []
    for x in offers:
        res.append(x)
    # print(res)
    return res
def get_attendence(term,usn,sem):
    collection = db.dhi_student_attendance
    attendence = collection.aggregate([
            {"$match":{"students.usn":usn}},
            {"$unwind":"$departments"},
            {"$unwind":"$students"},
            {"$match":{"students.usn":usn,"departments.termName":sem,"academicYear":term}},
            {"$project":{"total_classes":"$students.totalNumberOfClasses","present":"$students.presentCount","absent":"$students.absentCount","percentage":"$students.percentage","_id":0,"courseCode":1,"courseName":1}}
        ])
    res = []
    for x in attendence:
        res.append(x)
    pp.pprint(res)
    #return res
get_attendence("2017-18","4MT16CS105","Semester 4")
def get_ia_marks(term, usn,sem):
    collection = db.dhi_internal
    scores = collection.aggregate([
        {"$match":{"studentScores.usn":usn,"departments.termName":sem}},
        {"$unwind":"$studentScores"},
        {"$match":{"studentScores.usn":usn,"academicYear":term}},
        {"$project":{"obtained":"$studentScores.totalScore","outof":"$evaluationParameters.collegeMaxMarks","iaNumber":1,"courseName":1,"courseCode":1,"_id":0}},
        ])
    res = []
    for score in scores:
        res.append(score)
    #pprint.pprint(res)
    return res

def get_ia_marks_total(term, usn,sem):
    collection = db.dhi_internal
    scores = collection.aggregate([
        {"$match":{"studentScores.usn":usn,"departments.termName":sem}},
        {"$unwind":"$studentScores"},
        {"$match":{"studentScores.usn":usn,"academicYear":term}},
        {"$project":{"obtained":"$studentScores.totalScore","outof":"$evaluationParameters.collegeMaxMarks","iaNumber":1,"courseName":1,"courseCode":1,"_id":0}},
        {"$group":{"_id":"$courseName","max":{"$sum":"$outof"},"got":{"$sum":"$obtained"},"courseCode":{"$first":"$courseCode"}}},
        {"$project":{"courseName":"$_id","courseCode":1,"max":1,"got":1,"_id":0}}
        ])
    res = []
    for score in scores:
        res.append(score)
    #pprint.pprint(res)
    return res
#get_ia_marks_total("4MT16CS105","Semester 4")

def get_emp_id(email):
    collection = db.dhi_user
    usn = collection.aggregate([
    {"$match":{"email":email}},
    {"$project":{"_id":0,"employeeGivenId":1}}
    ])
    res = []
    for x in usn:
        res = x["employeeGivenId"]
    #print(res)
    return res

# def get_employee_():
#     collection = db.dhi_student_attendance
#     subjects = collection('dhi_student_attendance').aggregate([
#     {"$match":{"faculties.employeeGivenId":"MEC625","academicYear":"2017-18"}},
#     {"$unwind":"$departments"},
#     {"$match":{"departments.termName":"Semester 4"}},
#     {"$project":{"students":{"$size":"$students"},"_id":0,"courseCode":1,"courseName":1}}
#     ])

def get_emp_subjects(empid,term,sem):
    collection = db.dhi_internal
    marks = collection.aggregate([
    {"$match":{"faculties.facultyGivenId":empid,"academicYear":term,"departments.termName":sem}},
    {"$unwind":"$departments"},
    {"$unwind":"$studentScores"},
    {"$match":{"studentScores.totalScore":{"$gt":0}}},
    {"$group":{"_id":"$courseCode","totalMarks":{"$sum":"$studentScores.totalScore"},"maxMarks":{"$sum":"$evaluationParameters.collegeMaxMarks"},
    "courseCode":{"$first":"$courseCode"},"courseName":{"$first":"$courseName"}}}
    ])
    res = []
    for mark in marks:
        res.append(mark)
    pp.pprint(res)

def get_emp_subjects_ia_wise(empid,term,sem):
    collection = db.dhi_internal
    marks = collection.aggregate([
    {"$match":{"faculties.facultyGivenId":empid,"academicYear":term,"departments.termName":sem}},
    {"$unwind":"$departments"},
    {"$unwind":"$studentScores"},
    {"$match":{"studentScores.totalScore":{"$gt":0}}},
    {"$group":{"_id":{"iaNumber":"$iaNumber","courseCode":"$courseCode"},"totalMarks":{"$sum":"$studentScores.totalScore"},"maxMarks":{"$sum":"$evaluationParameters.collegeMaxMarks"},
    "courseCode":{"$first":"$courseCode"},"courseName":{"$first":"$courseName"}}},
    {"$project":{"_id":0,"iaNumber":"$_id.iaNumber","courseCode":1}}
    ])
    res = []
    for mark in marks:
        res.append(mark)
    pp.pprint(res)
#get_emp_subjects_ia_wise("CIV598","2017-18","Semester 3")



