/**
 *
 * mongodb-lock.js - Use your existing MongoDB as a local lock.
 *
 * Copyright (c) 2015 Andrew Chilton
 * - http://chilts.org/
 * - andychilton@gmail.com
 *
 * License: http://chilts.mit-license.org/2015/
 *
**/

var crypto = require('crypto')

var DEFAULT_LOCK_TIMEOUT = 30000;

// some helper functions
function id() {
  return crypto.randomBytes(16).toString('hex')
}

module.exports = function(mongoDbClient, collectionName, lockName, opts) {
  return new Lock(mongoDbClient, collectionName, lockName, opts)
}

// the Lock object itself
function Lock(mongoDbClient, collectionName, lockName, opts) {
  if ( !mongoDbClient ) {
    throw new Error("mongodb-lock: provide a mongodb.MongoClient")
  }
  if ( !collectionName ) {
    throw new Error("mongodb-lock: provide a collectionName")
  }
  if ( !lockName ) {
    throw new Error("mongodb-lock: provide a lockName")
  }
  opts = opts || {}

  var self = this

  self.col = mongoDbClient.collection(collectionName)
  self.name = lockName
  self.timeout = opts.timeout || DEFAULT_LOCK_TIMEOUT;
}

Lock.prototype.ensureIndexes = function(callback) {
  var self = this

  self.col.ensureIndex({ name : 1 }, { unique : true }, function(err) {
    if (err) return callback(err)
    callback()
  })
}

Lock.prototype.acquire = function(callback) {
  var self = this

  var now = Date.now()

  // firstly, expire any locks if they have timed out
  var q1 = {
    name   : self.name,
    expire : { $lt : now },
  }
  var u1 = {
    $set : {
      name    : self.name + ':' + now,
      expired : now,
    },
  }
  self.col.findAndModify(q1, undefined /* sort order */, u1, function(err, oldLock) {
    if (err) return callback(err)

    // now, try and insert a new lock
    var code = id()
    var doc = {
      name     : self.name,
      code     : code,
      expire   : now + self.timeout,
      inserted : now,
    }

    self.col.insert(doc, function(err, docs) {
      if (err) {
        if (err.code === 11000 ) {
          // there is currently a valid lock in the datastore
          return callback(null, null)
        }
        // don't know what this error is
        return callback(err)
      }

      var doc = docs.ops[0]
      callback(null, doc.code)
    })
  })
}

Lock.prototype.release = function release(code, callback) {
  var self = this

  var now = Date.now()

  // Expire this lock if it is still valid
  var q1 = {
    code    : code,
    expire  : { $gt : now },
    expired : { $exists : false },
  }
  var u1 = {
    $set : {
      name    : self.name + ':' + now,
      expired : now,
    },
  }
  self.col.findAndModify(q1, undefined /* sort order */, u1, function(err, oldDoc) {
    if (err) return callback(err)

    if ( !oldDoc.value ) {
      // there was nothing to unlock
      return callback(null, false)
    }

    // unlocked correctly
    return callback(null, true)
  })
}

Lock.prototype.extend = function(code, extension, cb) {
  var self = this;
  var extension = extension || DEFAULT_LOCK_TIMEOUT;

  var now = Date.now();

  // Add extra time to lock if it is still valid.
  var query = {
    code    : code,
    expire  : { $gt : now },
    expired : { $exists : false },
  }
  var update = {
    $inc : {
      expire : extension
    },
  }
  self.col.findAndModify(query, undefined, update, function(err, oldDoc) {
    if (err) {
      return cb(err);
    }
    if(! oldDoc.value) {
      return cb(null, false);
    }
    return cb(null, true);
  });
}