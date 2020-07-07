use test

dateMap =  {$map: {
   input: { $range: [
     {$floor: {$divide: [{$toLong: "$start"}, 86400000]}},
     {$add: [{$floor: {$divide: [{$toLong: "$end"}, 86400000]}}, 1]}
   ]},
   as: "d",
   in: { $toDate: {$multiply: ["$$d", 86400000]}}
 }}
project = {$project: { "policy":1,"level":1,"days":dateMap}}
unwind = {$unwind: "$days"}
group = {$group: { "_id":"$days","policyLevel":{$max:"$level"}}}
sort = {$sort:{"_id":1}}
match = {$match:{"_id":{$gte:ISODate("2020-01-15T00:00:00Z"),$lte:ISODate("2020-03-15T00:00:00Z")}}}
regroup={$group:{"_id":"1","policies":{$push:{"day":"$_id","level":"$policyLevel"}}}}
setocc=
{$addFields:
  {"policy":
    {$function:
        {
           body: function(res) {
           levels={};
           firstLevel="";
           res.forEach((doc) => {
                      level=doc["level"];
                      if (level!=firstLevel) {
                              firstLevel=level;
                              if (level in levels) { levels[level]=levels[level]+1; }
                      else {levels[level]=0;}
                      }
                      doc["level"]=level+"'".repeat(levels[level]);
            });
            return res;                  
         },
           args: [ "$policies"],
           lang: "js"
          }
        }
}}
project2={$project:{"_id":0,"policy":1}}
unwindfinal={$unwind:"$policy"}
project3={$project:{"_id":"$policy.day","policyLevel":"$policy.level"}}
pipeline=[project,unwind,group,sort,match,regroup,setocc,project2,unwindfinal,project3]
db.policies.aggregate(pipeline) 

groupend={$group:{_id:"$policyLevel",dateStart:{$min:"$_id"},dateEnd:{$max:"$_id"}}}
pipeline2=[project,unwind,group,sort,match,regroup,setocc,project2,unwindfinal,project3,groupend]
db.policies.aggregate(pipeline2)