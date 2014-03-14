var EventEmitter = require('events').EventEmitter;
var util = require('util');
var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos');
var _ = require('lodash');
var tv4 = require('tv4');
var mongo = require('mongodb');
var Target = require('./lib/target').Target;
var schemas = require('./lib/schemas');
var properties = require('./lib/properties');

var mongoConf = {
  host: '127.0.0.1',
  port: mongo.Connection.DEFAULT_PORT,
  dbname: 'qos',
  collection: 'queue'
};

var targets = {};

var db = null;

/**
 * QOS configuration
 * @param {Object|Function} conf
 * @param {Function} done
 */
exports.configure = function (conf, done) {
  if (_.isFunction (conf)) {
    done = conf;
    conf = {};
  }
  conf = conf || {};

  if (!tv4.validate(conf, schemas.conf)) {
    return logger.warn('invalid configuration; use of default one', {conf: conf, err: tv4.error, defaultConf: conf});
  }

  if (conf.debug) properties.debug = true;
  if (conf.mongo) _.assign(mongoConf, conf.mongo);
  if (conf.timeout) properties.timeout = conf.timeout;
  logger.info('use configuration', {conf: conf});

  /* connection to the database */
  var server = new mongo.Server(mongoConf.host, mongoConf.port, {auto_reconnect: true});
  var client = new mongo.MongoClient(server);
  client.open(function (err, client) {
    if (err) {
      var errid = logger.error('cant connect database', {conf: mongoConf, err: err});
      return done && done({code: 'MONGOERR', errid: errid});
    }
    logger.info('connected to database', {conf: mongoConf});
    db = client.db(mongoConf.dbname);
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

  var bare = hubiquitus.utils.aid.bare(to);
  var target = targets[bare];
  if (!target) {
    target = targets[bare] = new Target(bare);
    target.on('queue pop', function (item) {
      exports.send(item.from, item.to, item.content, item.done);
    });
  }

  if (target.ok()) {
    target.notifyReq();
    var date = Date.now();

    var timeout = setTimeout(function () {
      target.notifyRes(properties.timeout);
    }, properties.timeout);

    hubiquitus.send(from, to, content, properties.processingTimeout, function (err) {
      var time = Date.now() - date;
      clearTimeout(timeout);
      logger.trace('response from target', {target: to, err: err});
      if (!err || err.code !== 'TIMEOUT') {
        if (time < properties.timeout) target.notifyRes(time);
        done && done(err);
      } else {
        target.queue({from: from, to: to, content: content, done: done});
      }
    }, {qos: true});
  } else {
    logger.trace('target not opened, message queued', {target: target.id, rate: target.rate});
    target.queue({from: from, to: to, content: content, done: done});
  }
};

/**
 * Manually inject message to queue
 * @param {String} from
 * @param {String} to
 * @param {Object} content
 * @param {Function} [done]
 */
exports.persist = function (from, to, content, done) {
  var collection = db.collection(mongoConf.collection);

  var toPersist = {
    type: 'out',
    date: Date.now(),
    req: {from: from, to: to, content: content},
    container: {
      ID: hubiquitus.properties.ID
    }
  };
  collection.insert(toPersist, {w: 1}, function (err, records) {
    var id = (_.isArray(records) && records.length > 0) ? records[0]._id : null;
    if (err || id === null) {
      logger.warn('message manual persisting error, will not be processed !', err);
      done && done({code: 'MONGOERR', message: 'couldnt queue message to process, stop processing'});
    } else {
      done && done(null, {
        done: function () {
          collection.remove({_id: id}, function (err) {
            if (err) {
              logger.warn('safe message removal error', err);
            }
          });
        }
      });
    }
  });
};

/**
 * Persists message, call done and send it. Flush queue at ack.
 * @param {String} from
 * @param {String} to
 * @param {Object} content
 * @param {Function} [done]
 */
exports.persistAndSend = function (from, to, content, done) {
  exports.persist(from, to, content, function (err, res) {
    if (err) {
      logger.warn('failed to persist message', err);
      return done && done(err);
    }

    done && done();
    exports.send(from, to, content, function (err) {
      if (err) {
        return logger.warn('failed to send message safely', err);
      }
      res.done && res.done();
    });
  });
};

/**
 * Middleware
 * @param {String} type
 * @param {Object} msg
 * @param {Function} next
 */
exports.middleware = function (type, msg, next) {
  if (type === 'req_in' && msg.headers.qos) {
    middlewareSafeIn(msg, next);
  } else if (type === 'res_out' && msg.headers.qos) {
    middlewareSafeOut(msg);
  } else {
    next();
  }
};

/**
 * Middlewares handling safed sent messages.
 * Enqueues messages in mongo collection and sends an ACK.
 * @param {Object} req
 * @param {Function} next
 */
function middlewareSafeIn(req, next) {
  logger.trace('middleware processing request...', {req: req});
  if (Date.now() - req.date > properties.timeout) {
    return logger.trace('timeout excedeed !', {req: req});
  }

  var collection = db.collection(mongoConf.collection);
  var toPersist = {
    type: 'in',
    date: Date.now(),
    err: null,
    req: _.omit(req, 'reply'),
    container: {
      ID: hubiquitus.properties.ID
    }
  };
  collection.insert(toPersist, {w: 1}, function (err, records) {
    var id = (_.isArray(records) && records.length > 0) ? records[0]._id : null;

    if (err || id === null) {
      logger.warn('safe message queueing error, will not be processed !', err);
      req.reply({code: 'MONGOERR', message: 'couldnt queue message to process, stop processing'});
    } else {
      req.headers.qos_id = id;
      req.reply();
      next();
    }
  });
}

/**
 * Middleware handling safed sent messages after processing.
 * Unqueue the processed message from the mongo collection.
 * @param {Object} res
 */
function middlewareSafeOut(res) {
  logger.trace('middleware processing response...', {res: res});
  var collection = db.collection(mongoConf.collection);
  if (res.err) {
    collection.update({_id: res.headers.qos_id}, {'$set': {err: res.err}}, function (err) {
      if (err) {
        return logger.warn('safe message update error', err);
      }
    });
  } else {
    collection.remove({_id: res.headers.qos_id}, function (err) {
      if (err) {
        return logger.warn('safe message removal error', err);
      }
    });
  }
  delete res.headers.qos_id;
}

/**
 * Batch
 */
exports.batch = require('./lib/batch');
