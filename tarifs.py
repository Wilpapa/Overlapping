# extracts daily policies between 2 dates, from overlapping policies stored by dates intervals
#
# input data (test.policies)
#{
#        "_id" : ObjectId("5efb1138fdf7f9c40e35388a"),
#        "start" : ISODate("2020-01-01T00:00:00Z"),
#        "end" : ISODate("2020-05-20T00:00:00Z"),
#        "level" : "A"
#}
#
# output data (intermediate, aggregation pipeline)
#{ "_id" : ISODate("2020-01-28T00:00:00Z"), "policyLevel" : "A" }
#{ "_id" : ISODate("2020-01-29T00:00:00Z"), "policyLevel" : "A" }
#{ "_id" : ISODate("2020-01-30T00:00:00Z"), "policyLevel" : "B" }
#{ "_id" : ISODate("2020-01-31T00:00:00Z"), "policyLevel" : "B" }
#{ "_id" : ISODate("2020-02-01T00:00:00Z"), "policyLevel" : "C" }
#{ "_id" : ISODate("2020-02-02T00:00:00Z"), "policyLevel" : "B" }
#{ "_id" : ISODate("2020-02-03T00:00:00Z"), "policyLevel" : "A" }
#
# output data (after final processing of policy level)
#{ "_id" : ISODate("2020-01-28T00:00:00Z"), "policyLevel" : "A" }
#{ "_id" : ISODate("2020-01-29T00:00:00Z"), "policyLevel" : "A" }
#{ "_id" : ISODate("2020-01-30T00:00:00Z"), "policyLevel" : "B" }
#{ "_id" : ISODate("2020-01-31T00:00:00Z"), "policyLevel" : "B" }
#{ "_id" : ISODate("2020-02-01T00:00:00Z"), "policyLevel" : "C" }
#{ "_id" : ISODate("2020-02-02T00:00:00Z"), "policyLevel" : "B'" }
#{ "_id" : ISODate("2020-02-03T00:00:00Z"), "policyLevel" : "A'" }

# guillaume at mongodb.com 20200630 ($project stage by Sylvain Chambon)

from datetime import datetime
import pymongo
import string

#dates to look for (lower, upper bound)
START_TIME = datetime.fromisoformat("2020-01-15")
END_TIME = datetime.fromisoformat("2020-03-15")

# MongoDB aggregation pipeline to extract higher policy level per day
pipeline=[
        {
                "$project" : {
                        "policy" : 1,
                        "level" : 1,
                        "days" : {
                                "$map" : {
                                        "input" : {
                                                "$range" : [
                                                        {
                                                                "$floor" : {
                                                                        "$divide" : [
                                                                                {
                                                                                        "$toLong" : "$start"
                                                                                },
                                                                                86400000
                                                                        ]
                                                                }
                                                        },
                                                        {
                                                                "$add" : [
                                                                        {
                                                                                "$floor" : {
                                                                                        "$divide" : [
                                                                                                {
                                                                                                    "$toLong" : "$end"
                                                                                                },
                                                                                                86400000
                                                                                        ]
                                                                                }
                                                                        },
                                                                        1
                                                                ]
                                                        }
                                                ]
                                        },
                                        "as" : "d",
                                        "in" : {
                                                "$toDate" : {
                                                        "$multiply" : [
                                                                "$$d",
                                                                86400000
                                                        ]
                                                }
                                        }
                                }
                        }
                }
        },
        {
                "$unwind" : "$days"
        },
        {
                "$group" : {
                        "_id" : "$days",
                        "policyLevel" : {
                                "$max" : "$level"
                        }
                }
        },
        {
                "$sort" : {
                        "_id" : 1
                }
        },
        {
                "$match" : {
                        "_id" : {
                                "$gte" : START_TIME,
                                "$lte" : END_TIME
                        }
                }
        }
]

# Extract data for daily policy level from  MongoDB
connection = pymongo.MongoClient()
res = list(connection.test.policies.aggregate(pipeline))

# Computes re-appearance of policy level (A,A',A'', etc.)
levels={}
firstLevel=""
for doc in res:
        level=doc["policyLevel"]
        if (level!=firstLevel):
                firstLevel=level
                levels[level]=levels[level]+1 if level in levels else 0
        doc["policyLevel"]=level + "'"*levels[level]
        print(doc)      
