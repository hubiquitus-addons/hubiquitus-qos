var EventEmitter = require('events').EventEmitter;
var util = require('util');
var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos');
var _ = require('lodash');
var tv4 = require('tv4');
var mongo = require('mongodb');
var schemas = require('./lib/schemas');

var conf = {
  timeout: 2000,
  mongo: {
    host: '127.0.0.1',
    port: mongo.Connection.DEFAULT_PORT,
    dbname: 'qos',
    collection: 'queue'
  }
};

var db = null;

/**
 * QOS configuration
 * @param {Object|Function} _conf
 * @param {Function} done
 */
exports.configure = function (_conf, done) {
  if (_.isFunction (_conf)) {
    done = _conf;
    _conf = {};
  }
  _conf = _conf || {};

   if (!tv4.validate(_conf, schemas.conf)) {
    return logger.warn('invalid configuration; use of default one', {conf: _conf, err: tv4.error, defaultConf: conf});
  }

  _.assign(conf, _conf);
  logger.info('use configuration', {conf: conf});

  /* connection to the database */
  var server = new mongo.Server(conf.mongo.host, conf.mongo.port, {auto_reconnect: true});
  var client = new mongo.MongoClient(server);
  client.open(function (err, client) {
    if (err) {
      var errid = logger.error('cant connect database', {conf: conf.mongo, err: err});
      return done && done({code: 'MONGOERR', errid: errid});
    }
    logger.info('connected to database', {conf: conf.mongo});
    db = client.db(conf.mongo.dbname);
    done && done();
  });
};

/**
 * Send a message
 * @param {String} from
 * @param {String} to
 * @param {Object} [content]
 * @param {Function} [done]
 */
exports.send = function (from, to, content, done) {
  logger.trace('send message with qos', {from: from, to: to, content: content, done: !!done});

  hubiquitus.send(from, to, content, conf.timeout, function (err) {
    logger.trace('response from target', {target: target.id, err: err});
    done && done(err);
  }, {safe: true});
};

/**
 * Middleware
 * @param {String} type
 * @param {Object} req
 * @param {Function} next
 */
exports.middleware = function (type, req, next) {
  if (type !== 'req_in' || !req.headers.safe) return next();

  logger.trace('middleware processing request...', {req: req});
  if (Date.now() - req.date > req.timeout) {
    return logger.trace('timeout excedeed !', {req: req});
  }

  var collection = db.collection(conf.mongo.collection);
  var reqToPersist = _.omit(req, 'reply');
  collection.insert({date: Date.now(), req: reqToPersist}, {safe: true}, function (err) {
    if (err) {
      logger.trace('safe message queueing error, will not be processed !');
      req.reply({code: 'MONGOERR', message: 'couldnt queue message to process, stop processing'});
    } else {
      req.reply();
      next();
    }
  });
};
