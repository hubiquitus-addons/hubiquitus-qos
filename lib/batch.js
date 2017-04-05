var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:batch');
var mongo = require('mongodb');
var _ = require('lodash');
var tv4 = require('tv4');
var schemas = require('./schemas');
var properties = require('./properties').batch;
var MongoClient = require('mongodb').MongoClient;

var db = null;

exports.run = function (conf, cb) {
  conf = conf || {};

  if (!tv4.validate(conf, schemas.batchConf)) {
    logger.warn('invalid configuration; the default one will be used', {conf: conf, err: tv4.error});
  } else {
    _.assign(properties, conf);
  }
  logger.info('using configuration', {conf: properties});

  MongoClient.connect(properties.mongo, {mongos: {'auto_reconnect': true}} , function(err, _db) {
    if (err) {
      logger.error('cant connect database', {conf: properties.mongo, err: err});
      return cb && cb({code: 'MONGOERR', cause: err});
    }
    logger.info('connected to database', {conf: properties.mongo});
    db = _db;
    cb && cb();
    loop();
  });
};

function loop() {
  (function tick() {
    logger.trace('garbage collector iteration begins');

    var query = {
      date: {'$lt': (Date.now() - properties.gcTimeout)},
      retry: {'$lte': properties.gcRetryLimit},
      err: {'$exists': false},
      removalTime: {$exists:false}
    };
    var options = {sort: [['retry', 'asc'], ['date', 'asc']]};
    var stream = db.collection(properties.collection)
      .find(query, options)
      .limit(properties.gcLimit)
      .stream();

    var streamed = 0;
    var processed = 0;
    var streamOver = false;
    var streamErr = false;
    var nextTick = null;

    stream.on('data', function (item) {
      streamed++;
      logger.trace('timeout reached; try to ping handler...', {item: item});
      hubiquitus.monitoring.pingContainer(item.container.ID, function (err) {
        if (err && err.code === 'TIMEOUT') {
          logger.trace('handler\'s container dead; request replay', {item: item});
          replay(item, function () {
            processed++;
            checkStatus();
          });
        } else if (!err) {
          logger.trace('handler\'s container alive; let\'s give him more time', {item: item});
          processed++;
          checkStatus();
        } else {
          logger.warn('handler\'s container ping returns error', {item: item, err: err});
          processed++;
          checkStatus();
        }
      });
    });

    stream.on('close', function () {
      logger.trace('garbage collector iteration ends');
      streamOver = true;
      checkStatus();
    });

    stream.on('error', function (err) {
      logger.error('garbage collector iteration error', {err: err});
      streamErr = true;
      checkStatus();
    });

    function checkStatus() {
      if ((streamErr || (streamOver && processed >= streamed)) && nextTick === null) {
        logger.trace(processed + ' items processed, next tick in ' + properties.gcInterval + 'ms');
        nextTick = setTimeout(tick, properties.gcInterval);
      }
    }
  })();
}

function replay(item, done) {
  var req = item.req;
  var to = hubiquitus.utils.aid.bare(req.to);
  hubiquitus.send(req.from, to, req.content, properties.gcReplayTimeout, function (err) {
    if (!err) {
      db.collection(properties.collection).update({_id: item._id}, {'$set': {removalTime: new Date()}}, function (err) {
        if (err) {
          logger.error('failed to set removalTime for replayed req from queue, may be processed twice !', {item: item, err: err});
        }
        done && done();
      });
    } else {
      logger.trace('failed to replay req, remains in queue', {err: err, item: item});
      db.collection(properties.collection).update({_id: item._id}, {$inc: {retry: 1}}, function (err) {
        if (err) {
          logger.warn('failed to update retry field of item', {err: err, item: item});
        }
        done && done();
      });
    }
  }, _.assign(req.headers, {qos: true, qos_id: hubiquitus.utils.uuid(), qos_retry: item.retry + 1}));
}
