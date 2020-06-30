# extracts daily tariffs between 2 dates, from overlapping tariffs stored by dates intervals
#
# input data (bollore.overlap)
#{
#        "_id" : ObjectId("5efb1138fdf7f9c40e35388a"),
#        "start" : ISODate("2020-01-01T00:00:00Z"),
#        "end" : ISODate("2020-05-20T00:00:00Z"),
#        "tariff" : 5,
#        "level" : "A"
#}
#
# output data (intermediate, aggregation pipeline)
#{ "_id" : ISODate("2020-01-28T00:00:00Z"), "tariffLevel" : "A" }
#{ "_id" : ISODate("2020-01-29T00:00:00Z"), "tariffLevel" : "A" }
#{ "_id" : ISODate("2020-01-30T00:00:00Z"), "tariffLevel" : "B" }
#{ "_id" : ISODate("2020-01-31T00:00:00Z"), "tariffLevel" : "B" }
#{ "_id" : ISODate("2020-02-01T00:00:00Z"), "tariffLevel" : "C" }
#{ "_id" : ISODate("2020-02-02T00:00:00Z"), "tariffLevel" : "B" }
#{ "_id" : ISODate("2020-02-03T00:00:00Z"), "tariffLevel" : "A" }
#
# output data (after final processing of tariff level)
#{ "_id" : ISODate("2020-01-28T00:00:00Z"), "tariffLevel" : "A" }
#{ "_id" : ISODate("2020-01-29T00:00:00Z"), "tariffLevel" : "A" }
#{ "_id" : ISODate("2020-01-30T00:00:00Z"), "tariffLevel" : "B" }
#{ "_id" : ISODate("2020-01-31T00:00:00Z"), "tariffLevel" : "B" }
#{ "_id" : ISODate("2020-02-01T00:00:00Z"), "tariffLevel" : "C" }
#{ "_id" : ISODate("2020-02-02T00:00:00Z"), "tariffLevel" : "B'" }
#{ "_id" : ISODate("2020-02-03T00:00:00Z"), "tariffLevel" : "A'" }

# guillaume at mongodb.com 20200630 ($project stage by Sylvain Chambon)

from datetime import datetime
import pymongo
import string

#dates to look for (lower, upper bound)
START_TIME = datetime.fromisoformat("2020-01-15")
END_TIME = datetime.fromisoformat("2020-03-15")

# MongoDB aggregation pipeline to extract higher tariff level per day
pipeline=[
        {
                "$project" : {
                        "tariff" : 1,
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
                        "tariffLevel" : {
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

# Extract data for daily tariff level from  MongoDB
connection = pymongo.MongoClient()
res = list(connection.test.overlap.aggregate(pipeline))

#array of tariffs levels
levels={}
indices=list(string.ascii_uppercase)
for value in indices:
    levels[value]=-1

# Computes re-appearance of tariff level (A,A',A'', etc.)
firstLevel=""
for doc in res:
    level=doc["tariffLevel"]
    if (level!=firstLevel):
        levels[level]+=1
        firstLevel=level
    newLevel = level + "'"*levels[level]
    doc["tariffLevel"]=newLevel
    print(doc)

