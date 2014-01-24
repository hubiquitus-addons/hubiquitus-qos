var hubiquitus = require('hubiquitus-core');
var hubiquitusQos = require('./index');
var logger = hubiquitus.logger('hubiquitus:addons:qos:batch');
hubiquitus.logger.enable('hubiquitus:addons:qos:batch', 'trace');
var commander = require('commander');
var mongo = require('mongodb');

commander
  .version('0.0.1')
  .option('-d, --debug', 'Debug')
  .option('-disc, --discovery [str]', 'Discovery addr')
  .option('-gc, --gc-interval [n]', 'GC interval', parseInt)
  .option('-t, --timeout [n]', 'Processing timeout', parseInt)
  .option('-mhost, --mongo-host [str]', 'Mongo host')
  .option('-mport, --mongo-port [n]', 'Mongo port', parseInt)
  .option('-mdb, --mongo-dbname [str]', 'Mongo dbname')
  .option('-mcoll, --mongo-collection [str]', 'Mongo collection')
  .parse(process.argv);

var conf = {
  debug: commander.debug || false,
  discovery: commander.discovery || '',
  gcInterval: commander.gcInterval || 60000,
  timeout: commander.timeout || 30000,
  mongo: {
    host: commander.mongoHost || '127.0.0.1',
    port: commander.mongoPort || mongo.Connection.DEFAULT_PORT,
    dbname: commander.mongoDbname || 'qos',
    collection: commander.mongoCollection || 'queue'
  }
};
logger.info('using configuration', {conf: conf});

hubiquitus.start({discoveryAddr: conf.discovery});

var db = null;

var server = new mongo.Server(conf.mongo.host, conf.mongo.port, {auto_reconnect: true});
var client = new mongo.MongoClient(server);
client.open(function (err, client) {
  if (err) {
    logger.error('cant connect database', {conf: conf.mongo, err: err});
    process.exit(1);
  }
  logger.info('connected to database', {conf: conf.mongo});
  db = client.db(conf.mongo.dbname);
  main();
});

function main() {
  setInterval(function () {
    logger.trace('garbage collector iteration begins');
    var query = {
      $or: [
        {date: {$lt: (Date.now() - conf.timeout)}},
        {error: true}
      ]
    };

    var cursor = db.collection(conf.mongo.collection).find(query);
    cursor.count(function (err, count) {
      if (err) {
        return logger.error('garbage collector iteration error', {query: query, err: err});
      }

      logger.trace('garbage collector iteration processing ' + count + ' items', {query: query});

      var stream = cursor.stream();

      stream.on('data', function (item) {
        if (item.error) {
          logger.trace('item in error; request replay', {item: item});
          replay(item._id, item.req);
        } else {
          logger.trace('timeout reached; try to ping handler...', {item: item});
          hubiquitus.send('qosBatch', item.req.to, null, function (err) {
            if (err && err.code === 'TIMEOUT') {
              logger.trace('handler dead; request replay', {item: item});
              replay(item._id, item.req);
            } else if (!err) {
              logger.trace('handler alive; let\'s give him more time', {item: item});
            } else {
              logger.warn('handler ping returns error', {item: item, err: err});
            }
          }, {ping: true});
        }
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

function replay(safeId, req) {
  hubiquitusQos.send(req.from, hubiquitus.utils.aid.bare(req.to), req.content, function (err) {
    if (!err) {
      db.collection(conf.mongo.collection).remove({_id: safeId}, function (err) {
        if (err) {
          logger.error('failed to remove replayed req from queue, may be processed twice !', {_id: safeId, err: err});
        }
      });
    }
  });
}
