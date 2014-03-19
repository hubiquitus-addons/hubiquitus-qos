var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:batch');
var mongo = require('mongodb');
var _ = require('lodash');
var properties = require('./properties');

var conf = {
  debug: false,
  gcInterval: 60000,
  timeout: 30000,
  limit: 10000,
  mongo: {
    host: '127.0.0.1',
    port: mongo.Connection.DEFAULT_PORT,
    dbname: 'qos',
    collection: 'queue'
  }
};

var db = null;

exports.run = function (_conf, cb) {
  _conf = _conf || {};
  conf = _.assign(conf, _conf);
  logger.info('using configuration', {conf: conf});

  var server = new mongo.Server(conf.mongo.host, conf.mongo.port, {auto_reconnect: true});
  var client = new mongo.MongoClient(server);
  client.open(function (err, client) {
    if (err) {
      logger.error('cant connect database', {conf: conf.mongo, err: err});
      return cb && cb({code: 'MONGOERR', cause: err});
    }
    logger.info('connected to database', {conf: conf.mongo});
    db = client.db(conf.mongo.dbname);
    cb && cb();
    loop();
  });
};

function loop() {
  (function tick() {
    logger.trace('garbage collector iteration begins');

    var query = {date: {'$lt': (Date.now() - conf.timeout)}, err: {'$exists': false}};
    var options = {sort: [['retry', 'asc'], ['date', 'asc']]};
    var stream = db.collection(conf.mongo.collection).find(query, options).limit(conf.limit).stream();

    var streamed = 0;
    var processed = 0;
    var streamOver = false;
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
    });

    function checkStatus() {
      if (streamOver && processed >= streamed && nextTick === null) {
        logger.trace(processed + ' items processed, next tick in ' + conf.gcInterval + 'ms');
        nextTick = setTimeout(tick, conf.gcInterval);
      }
    }
  })();
}

function replay(item, done) {
  var req = item.req;
  var to = hubiquitus.utils.aid.bare(req.to);
  hubiquitus.send(req.from, to, req.content, properties.processingTimeout, function (err) {
    if (!err) {
      db.collection(conf.mongo.collection).remove({_id: item._id}, function (err) {
        if (err) {
          logger.error('failed to remove replayed req from queue, may be processed twice !', {item: item, err: err});
        }
        done && done();
      });
    } else {
      logger.trace('failed to replay req, remains in queue', {err: err, item: item});
      db.collection(conf.mongo.collection).update({_id: item._id}, {$inc: {retry: 1}}, function (err) {
        if (err) {
          logger.warn('failed to update retry field of item', {err: err, item: item});
        }
        done && done();
      });
    }
  }, {qos: true, qos_retry: item.retry + 1});
}
