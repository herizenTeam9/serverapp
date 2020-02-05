from pymongo import MongoClient
import pprint as pp
import re
import bson
url="mongodb://localhost:27017/"
client=MongoClient(url)
db=client.dhi_analytics
#print(client.list_database_names())

#Returns All the academic years
def get_academic_year():
    dhi_internal = db.dhi_internal
    academicyear=dhi_internal.aggregate([
        {"$group":{"_id":"null","academicyear":{"$addToSet":"$academicYear"}}},
        {"$project":{"academicyear":"$academicyear","_id":0}}
    ])
    for year in academicyear:
        year=year['academicyear']
    return year

#returns all the semesters
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

#returns the usn of the requested email if present
def get_student_usn(email):
    collection = db.dhi_user
    usn = collection.aggregate([
        {"$match":{"email":email}},
        {"$project":{"_id":0,"usn":1}}
    ])
    res = []
    for x in usn:
        if x["usn"]:
            res = x["usn"]
    #print(res)
    return res

#returns all the placement offers of the student of the passed USN in the term
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

#returns the attendence of the student
#mistake 
def get_attendence(term,usn,sem):
    collection = db.dhi_student_attendance
    attendence = collection.aggregate([
            {"$match":{"students.usn":usn}},
            {"$unwind":"$departments"},
            {"$unwind":"$students"},
            {"$match":{"students.usn":usn,"departments.termName":sem,"academicYear":term}},
            {"$project":{"total_classes":"$students.totalNumberOfClasses","present":"$students.presentCount","absent":"$students.absentCount",
            "percentage":"$students.percentage","_id":0,"courseCode":1,"courseName":1}}
        ])
    res = []
    for x in attendence:
        if x not in res:
            res.append(x)
    #pp.pprint(res)
    return res

#returns the individual ia marks of a subject
def get_ia_marks(term, usn,sem,subject):
    collection = db.dhi_internal
    scores = collection.aggregate([
        {"$match":{"studentScores.usn":usn,"departments.termName":sem,"courseName":subject}},
        {"$unwind":"$studentScores"},
        {"$match":{"studentScores.usn":usn,"academicYear":term}},
        {"$project":{"obtained":"$studentScores.totalScore","outof":"$evaluationParameters.collegeMaxMarks","iaNumber":1,"courseName":1,"courseCode":1,"_id":0}},
        ])
    res = []
    for score in scores:
        res.append(score)
    #pp.pprint(res)
    return res
#get_ia_marks("2017-18","4MT16CS105","Semester 3")

#returns the summarised ia marks of all subjects of that sem,term
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
    #pp.pprint(res)
    return res
#get_ia_marks_total("2017-18","4MT16CS105","Semester 4")

#returns the empID of the given email if present
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


#returns the subjects,marks handled by the faculty of empID 
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

#returns ia wise of a subject
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

#placement details of a class handled by empID
def get_emp_sub_placement(empID,sub,term,sem):
    collection = db.dhi_student_attendance
    students = collection.aggregate([
        {"$match":{"academicYear":term,"faculties.employeeGivenId" : empID,"departments.termName":sem,"courseName":sub}},
        {"$unwind":"$students"},
        {"$group":{"_id":"$courseName","studentUSNs":{"$addToSet":"$students.usn"}}},
    ])
    res = []
    for x in students:
        res.append(x)
    
    filtered = []
    for x in res:
        for usn in x["studentUSNs"]:
            status = get_placed_details(usn)
            if status!=0:
                filtered.append(status)
    # print("filtered",filtered)
    # print(f"Placed Students :{len(filtered)},No.of Offers : {sum(filtered)}")
    return (len(filtered),sum(filtered))

#returns no of placement offers obtained by a student of passed usn
def get_placed_details(usn):
    collection = db.pms_placement_student_details
    people = collection.aggregate([
    {"$match":{"studentList.regNo":usn}},
    {"$unwind":"$studentList"},
    {"$match":{"studentList.regNo":usn}},
    ])
    res = []
    for x in people:
        res.append(x)
    return len(res)
#get_emp_subjects_ia_wise("CIV598","2017-18","Semester 3")
#get_emp_sub_placement("CSE638","INFORMATION AND NETWORK SECURITY","2017-18","Semester 8")

#returns the list of all department
def get_all_depts():
    collection = db.dhi_user
    depts = collection.aggregate([
        {"$match":{"roles.roleName":"FACULTY"}},
        {"$project":{"_id":0,"employeeGivenId":1}}
    ])
    res = []
    for d in depts:
        if "employeeGivenId" in d:
            res.append(d["employeeGivenId"])
    #print(len(res))
    dept = []
    for d in res:
        name = re.findall('([a-zA-Z]*).*',d)
        if name[0].upper() not in dept:
            dept.append(name[0].upper())
    dept.remove('ADM')
    dept.remove('EC')
    return dept

#returns the list of all faculties in a department
def get_faculties_by_dept(dept):
    collection = db.dhi_user
    pattern = re.compile(f'^{dept}')
    regex = bson.regex.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE 
    faculties = collection.aggregate([
        {"$match":{"roles.roleName":"FACULTY","employeeGivenId":{"$regex":regex}}},
        {"$project":{"employeeGivenId":1,"name":1,"_id":0}}
    ])
    res = [f for f in faculties]
    return res
#get_faculties_by_dept("CSE")
    