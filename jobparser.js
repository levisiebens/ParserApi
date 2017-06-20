var _ = require("lodash");
var assert = require("assert");
var MongoClient = require("mongodb").MongoClient;
var ObjectID = require('mongodb').ObjectID;

var url;
var jobCollectionName;
var newJobCollectionName;

module.exports = function(ctx, cb) {
  //Secrets
  url = ctx.secrets.dbconnection;
  jobCollectionName = ctx.secrets.jobcollectionname;
  newJobCollectionName = ctx.secrets.newjobcollectionname;

  //Parsed Data
  filters = JSON.parse(ctx.data.filters);
  companyFilterOut = JSON.parse(ctx.data.companyfilterout);
  titleFilterOut = JSON.parse(ctx.data.titlefilterout);
  var jobData = JSON.parse(ctx.data.parseddata);

  MongoClient.connect(url, function(err, db) {
    var jobCollection = db.collection(jobCollectionName);

    //Filter Jobs by title
    var results =_.filter(jobData, filterPredicate);

    //Filter out specific companies
    results = _.filter(results, companyFilterOutPredicate);

    //Filter out specific titles
    results = _.filter(results, titleFilterOutPredicate);

    //Iterate over filtered results.
    results.forEach(function (localJob) {
      
      //Attempt to see if the job is in the db already, if not add it to the db.
      jobCollection.find({"jobid": localJob.jobId}, function (err, docs) {
        docs.count(function(err, count) {
          
          //If we have no items that match, then add.
          if(count === 0) {
            MongoClient.connect(url, function(err, db1) {
              db1.collection(jobCollectionName).insertOne(localJob, insertError);
              db1.collection(newJobCollectionName).insertOne(localJob, insertError);
              db1.close();
            });
          }
        });
      });
     });

    db.close();
    cb(null, { status:"Success" });
  });
};

//Filter Predicate
var filters;
function filterPredicate (item) {
  for(var filterIterator = 0; filterIterator < filters.length; filterIterator++) {
    if(item.jobTitle.indexOf(filters[filterIterator].filter) > -1){
      return true;
    }
  }

  return false;
}

//Company Predicate
var companyFilterOut;
function companyFilterOutPredicate(item) {
  return filterOutItem(item.company, companyFilterOut);
}

//Title Predicate
var titleFilterOut;
function titleFilterOutPredicate(item) {
  return filterOutItem(item.jobTitle, titleFilterOut);
}

function insertError(err, result) {
  assert.equal(err, null);
}

function filterOutItem(item, filterOutList) {
  for(var filterOutIndex = 0; filterOutIndex < filterOutList.length; filterOutIndex++) {
    if(item.indexOf(filterOutList[filterOutIndex].filter) !== -1) {
      return false;
    }
  }
  return true;
}
