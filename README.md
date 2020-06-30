# Overlapping
 
 Reads a MongoDB database for a list of tariffs over a period of time and returns the higher tariff level for each day in the date range
 
 # Create test data (sample)
 
 Connect to MongoDB cluster using Mongo Shell and run the following commands :
 ```javascript
 doc=[{
	"start" : ISODate("2020-01-01T00:00:00Z"),
	"end" : ISODate("2020-05-20T00:00:00Z"),
  "tariff" : 5,
  "level" : "A"
},
{
	"start" : ISODate("2020-02-01T00:00:00Z"),
	"end" : ISODate("2020-02-28T00:00:00Z"),
  "tariff" : 4,
  "level" : "B"
},
{
	"start" : ISODate("2020-02-15T00:00:00Z"),
	"end" : ISODate("2020-03-10T00:00:00Z"),
  "tariff" : 3,
  "level" : "C"
}]
use test
db.overlap.insertMany(doc)
```

# Calculate tariff levels between 2 dates

Let's compute the daily tariff level for each day between Jan, 15th 2020 and Mar, 15th 2020.
The result should be :
* Level A between 1/15 and 1/31
* Level B between 2/1 and 2/14
* Level C between 2/15 and 3/10
* Level A again between 3/11 and 3/15

so tariff level list is ABCA. By convention, any re-appearing tariff level should have a different name (A',A'', etc.) - so we should have ABCA'

# MongoDB aggregation pipeline

In order to get the higher tariff level for each day (not yet taking care of reappearing tariff level notation A'), we can use the $map feature along with the $range. By switching dates to EPOCH value, computing range per slice of 1 day ($range) and multiplying each result again by the number of milliseconds in a day - 86 400 000 - ($map) :

```javascript
dateMap =  {$map: {
   input: { $range: [
     {$floor: {$divide: [{$toLong: "$start"}, 86400000]}},
     {$add: [{$floor: {$divide: [{$toLong: "$end"}, 86400000]}}, 1]}
   ]},
   as: "d",
   in: { $toDate: {$multiply: ["$$d", 86400000]}}
 }}
project = {$project: { "tariff":1,"level":1,"days":dateMap}}
unwind = {$unwind: "$days"}
group = {$group: { "_id":"$days","tariffLevel":{$max:"$level"}}}
sort = {$sort:{"_id":1}}
match = {$match:{_id:{$gte:ISODate("2020-01-15T00:00:00Z"),$lte:ISODate("2020-03-15T00:00:00Z")}}}
pipeline=[project,unwind,group,sort,match]
db.overlap.aggregate(pipeline) 
```

# Python code to compute A',A'', etc.

The python code wraps the aggregation pipeline and processes the output : each time a tariff level appears, it calculates the repetition number (0 for the first appearance).
Each tariff level is then stored by in the result adding as many single quote "'" as appeareances.

The end result for this sample is then :
```

{'_id': datetime.datetime(2020, 1, 15, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 16, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 17, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 18, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 19, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 20, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 21, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 22, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 23, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 24, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 25, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 26, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 27, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 28, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 29, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 30, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 1, 31, 0, 0), 'tariffLevel': 'A'}
{'_id': datetime.datetime(2020, 2, 1, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 2, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 3, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 4, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 5, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 6, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 7, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 8, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 9, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 10, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 11, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 12, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 13, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 14, 0, 0), 'tariffLevel': 'B'}
{'_id': datetime.datetime(2020, 2, 15, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 16, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 17, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 18, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 19, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 20, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 21, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 22, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 23, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 24, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 25, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 26, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 27, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 28, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 2, 29, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 1, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 2, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 3, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 4, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 5, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 6, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 7, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 8, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 9, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 10, 0, 0), 'tariffLevel': 'C'}
{'_id': datetime.datetime(2020, 3, 11, 0, 0), 'tariffLevel': "A'"}
{'_id': datetime.datetime(2020, 3, 12, 0, 0), 'tariffLevel': "A'"}
{'_id': datetime.datetime(2020, 3, 13, 0, 0), 'tariffLevel': "A'"}
{'_id': datetime.datetime(2020, 3, 14, 0, 0), 'tariffLevel': "A'"}
{'_id': datetime.datetime(2020, 3, 15, 0, 0), 'tariffLevel': "A'"}
```

Notice the A' in the last 5 lines
