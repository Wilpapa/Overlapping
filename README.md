# Overlapping
 
 Reads a MongoDB database for a list of tariffs over a period of time and returns the high tariff level for each day in the date range
 
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
use bollorelogistics
db.overlap.insertMany(doc)
```
