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

exports.run = function (_conf) {
  _conf = _conf || {};
  conf = _.assign(conf, _conf);
  logger.info('using configuration', {conf: conf});

  var server = new mongo.Server(conf.mongo.host, conf.mongo.port, {auto_reconnect: true});
  var client = new mongo.MongoClient(server);
  client.open(function (err, client) {
    if (err) {
      logger.error('cant connect database', {conf: conf.mongo, err: err});
      process.exit(1);
    }
    logger.info('connected to database', {conf: conf.mongo});
    db = client.db(conf.mongo.dbname);
    loop();
  });
};

function loop() {
  setInterval(function () {
    logger.trace('garbage collector iteration begins');
    var query = {
      date: {'$lt': (Date.now() - conf.timeout)}, err: null
    };

    var cursor = db.collection(conf.mongo.collection).find(query);
    cursor.count(function (err, count) {
      if (err) {
        return logger.error('garbage collector iteration error', {query: query, err: err});
      }

      logger.trace('garbage collector iteration... ' + count + ' items (total)', {query: query});

      var stream = cursor.limit(conf.limit).stream();

      stream.on('data', function (item) {
        logger.trace('timeout reached; try to ping handler...', {item: item});
        hubiquitus.monitoring.pingContainer(item.container.ID, function (err) {
          if (err && err.code === 'TIMEOUT') {
            logger.trace('handler\'s container dead; request replay', {item: item});
            replay(item._id, item.req);
          } else if (!err) {
            logger.trace('handler\'s container alive; let\'s give him more time', {item: item});
          } else {
            logger.warn('handler\'s container ping returns error', {item: item, err: err});
          }
        });
      });

      stream.on('close', function () {
        logger.trace('garbage collector iteration ends');
      });

      stream.on('error', function (err) {
        logger.error('garbage collector iteration error', {query: query, err: err});
      });
    });
  }, conf.gcInterval);
}

function replay(qosId, req) {
  hubiquitus.send(req.from, hubiquitus.utils.aid.bare(req.to), req.content, properties.processingTimeout, function (err) {
    if (!err) {
      db.collection(conf.mongo.collection).remove({_id: qosId}, function (err) {
        if (err) {
          logger.error('failed to remove replayed req from queue, may be processed twice !', {_id: qosId, err: err});
        }
      });
    } else {
      logger.error('failed to replay req, remains in queue', {err: err, req: req});
    }
  }, {qos: true});
}
