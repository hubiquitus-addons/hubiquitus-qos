var EventEmitter = require('events').EventEmitter;
var util = require('util');
var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos');
var _ = require('lodash');
var tv4 = require('tv4');
var MongoClient = require('mongodb').MongoClient;
var Target = require('./lib/target').Target;
var schemas = require('./lib/schemas');
var properties = require('./lib/properties');
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
    logger.warn('invalid configuration; the default one will be used', {conf: conf, err: tv4.error});
  } else {
    if (conf.mongo) properties.mongo = conf.mongo;
    if (conf.collection) properties.collection = conf.collection;
    if (conf.timeout) properties.timeout = conf.timeout;
    if (conf.processingTimeout) properties.processingTimeout = conf.processingTimeout;
  }
  logger.info('use configuration', {conf: properties});

  /* connection to the database */
  MongoClient.connect(properties.mongo, { useNewUrlParser: true, autoReconnect: true } , function(err, _client) {
    if (err) {
      logger.error('cant connect to database', {conf: properties.mongo, err: err});
      return done && done({code: 'MONGOERR'});
    }
    logger.info('connected to database', {conf: properties.mongo});
    db = _client.db();
    done && done();
  });
};

/**
 * Send a message
 * @param {String} from
 * @param {String} to
 * @param {Object} content
 * @param {Object} [headers={}]
 * @param {Function} done
 */
exports.send = function (from, to, content, headers, done) {
  if (_.isUndefined(done)) {
    done = headers;
    headers = {};
  }
  var bare = hubiquitus.utils.aid.bare(to);
  var target = targets[bare];
  if (!target) {
    target = targets[bare] = new Target(bare);
    target.on('queue pop', function (item) {
      internalSend(target, item);
    });
  }
  var uid = hubiquitus.utils.uuid();
  internalSend(target, {from: from, to: to, content: content, headers: headers, done: done, retry: 0, id: uid});
};

function internalSend(target, msg) {
  logger.trace('send message with qos', {msg: msg});

  if (msg.retry > 0) {
    var collection = db.collection(properties.collection);
    collection.findOne({_id: msg.id}, function (err, item) {
      if (err) {
        logger.error('error while checking if the message has already been persisted by the target before sending', {
          err: err, msg: msg});
        target.queue(msg);
      } else if (!item) {
        doSend();
      }
    });
  } else {
    doSend();
  }

  function doSend() {
    if (target.ok()) {
      target.notifyReq();
      var date = Date.now();

      var timeout = setTimeout(function () {
        msg.retry++;
        target.notifyRes(properties.timeout);
      }, properties.timeout);

      hubiquitus.send(msg.from, msg.to, msg.content, properties.processingTimeout, function (err) {
        var time = Date.now() - date;
        clearTimeout(timeout);
        logger.trace('response from target', {msg: msg, err: err});
        if (!err || err.code !== 'TIMEOUT') {
          if (time < properties.timeout) target.notifyRes(time);
          msg.done && msg.done(err);
        } else {
          target.queue(msg);
        }
      }, _.assign(msg.headers, {qos: true, qos_id: msg.id}));
    } else {
      logger.trace('target not opened, message queued', {target: target.id, rate: target.rate});
      target.queue(msg);
    }
  }
}

/**
 * Manually inject message to queue
 * @param {String} from
 * @param {String} to
 * @param {Object} content
 * @param {Object} headers
 * @param {Function} [done]
 */
exports.persist = function (from, to, content, headers, done) {
  var collection = db.collection(properties.collection);

  var toPersist = {
    type: 'out',
    date: Date.now(),
    retry: 0,
    req: {from: from, to: to, content: content, headers: headers},
    container: {
      ID: hubiquitus.properties.ID
    }
  };
  collection.insertOne(toPersist, {w: 1}, function (err, res) {
    var id = res.insertedCount?res.insertedId : null;
    if (err || id === null) {
      logger.warn('message manual persisting error, will not be processed !', err);
      done && done({code: 'MONGOERR', message: 'couldnt queue message to process, stop processing', cause: err});
    } else {
      done && done(null, {
        done: function () {
          collection.deleteOne({_id: id}, function (err) {
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
 * @param {Object} [headers={}]
 * @param {Function} done
 */
exports.persistAndSend = function (from, to, content, headers, done) {
  if (_.isUndefined(done)) {
    done = headers;
    headers = {};
  }

  exports.persist(from, to, content, headers, function (err, res) {
    if (err) {
      logger.warn('failed to persist message', err);
      return done && done(err);
    }

    done && done();
    exports.send(from, to, content, headers, function (err) {
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
    logger.trace('timeout excedeed !', {req: req});
    return req.reply({code: 'TIMEOUT', message: 'QOS Middleware intercepted message but timeout exceeded !'});
  }

  var collection = db.collection(properties.collection);
  var retry = 0;
  if (_.isNumber(req.headers.qos_retry)) {
    retry = req.headers.qos_retry;
  }
  var toPersist = {
    _id: req.headers.qos_id,
    type: 'in',
    date: Date.now(),
    retry: retry,
    req: _.omit(req, 'reply'),
    container: {
      ID: hubiquitus.properties.ID
    }
  };

  collection.replaceOne({_id:toPersist._id}, toPersist, {upsert: true}, function (err) {
    if (err) {
      logger.warn('safe message queueing error, will not be processed !', err);
      req.reply({code: 'MONGOERR', message: 'couldnt queue message to process, stop processing'});
    } else {
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
  var collection = db.collection(properties.collection);
  if (res.err) {
    collection.updateOne({_id: res.headers.qos_id}, {'$set': {err: res.err}}, function (err) {
      if (err) {
        return logger.warn('safe message update error', err);
      }
    });
  } else {
    collection.updateOne({_id: res.headers.qos_id}, {'$set': {removalTime: new Date()}}, function (err) {
      if (err) {
        return logger.warn('safe message update removalTime error', err);
      }
    });
  }
}

/**
 * Batch
 */
exports.batch = require('./lib/batch');
