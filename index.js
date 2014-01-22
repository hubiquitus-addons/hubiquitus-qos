var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos');
var _ = require('lodash');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

const timeout = 2000;

var targets = {};

/**
 * Update loop
 */

setInterval(function () {
  _.forEach(targets, function (target) {
    console.log(target.toString());
    target.updateRate();
    target.processQueue();
  });
}, 1000);

/**
 * Target
 */
var Target = (function () {

  /**
   * Target
   * @param id {String} target identity
   * @constructor
   */
  function Target(id) {
    this.id = id;
    this.computingRate = 500;
    this.rate = this.computingRate;
    this._sent = 0;
    this._queue = [];
    this._queueProcessIt = 0;
    this._queueOut = 0;
    this._queueOutScheduled = 0;
    this._queueIn = 0;
  }

  Target.prototype.notifyReq = function () {
    this._sent++;
  };

  Target.prototype.notifyRes = function (time) {
    if (time >= timeout) {
      this.computingRate -= 1;
    } else {
      this.computingRate += 1;
    }
  };

  Target.prototype.updateRate = function () {
    this._sent = 0;
    this._queueIn = 0;
    this._queueOutScheduled = 0;
    this._queueOut = 0;
    if (this.computingRate <= 0) this.computingRate = 1;
    this.rate = this.computingRate;
  };

  Target.prototype.isOpen = function () {
    return this._sent < this.rate;
  };

  Target.prototype.queue = function (item) {
    this._queueIn++;
    this._queue.push(item);
  };

  Target.prototype.processQueue = function () {
    var currentIt = ++this._queueProcessIt;
    var _this = this;
    (function unqueue() {
      if (_this._queue.length > 0 && _this.isOpen() && currentIt === _this._queueProcessIt) {
        _this._queueOutScheduled++;
        setImmediate(function () {
          var item = _this._queue.shift();
          _this._queueOut++;
          exports.send(item.from, item.to, item.content, item.done);
          unqueue();
        });
      }
    })();
  };

  Target.prototype.toString = function () {
    return 'Target ' + this.id + '; rate : ' + this.rate + ' msg/s\n' +
      '\t' + 'Messages sent : ' + this._sent + '\n' +
      '\t' + 'Queue : ' + this._queue.length + ' items '+
      '(' + this._queueIn + ' in; ' + this._queueOut + ' out / ' + this._queueOutScheduled + ' scheduled)\n' +
      '\t' + 'Computing rate : ' + this.computingRate;
  };

  return Target;
})();

/**
 * Send a message
 * @param {String} from
 * @param {String} to
 * @param {Object} [content]
 * @param {Function} [done]
 */
exports.send = function (from, to, content, done) {
  logger.trace('send message with qos', {from: from, to: to, content: content, done: !!done});

  var target = targets[to];
  if (!target) target = targets[to] = new Target(to);

  if (target.isOpen()) {
    logger.trace('target opened, sending request', {target: target.id, rate: target.rate});
    target.notifyReq();
    var date = Date.now();
    hubiquitus.send(from, to, content, timeout, function (err) {
      var time = Date.now() - date;
      logger.trace('response from target', {target: target.id, err: err});
      if (err && err.code === 'TIMEOUT') {
        target.queue({from: from, to: to, content: content, done: done});
        target.notifyRes(time);
      } else {
        done && done(err);
        if (!err) target.notifyRes(time);
      }
    }, {safe: true});
  } else {
    logger.trace('target not opened, queue message', {target: target.id, rate: target.rate});
    target.queue({from: from, to: to, content: content, done: done});
  }
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
  req.reply();
  next();
};
