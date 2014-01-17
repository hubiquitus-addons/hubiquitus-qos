var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos');
var _ = require('lodash');
var util = require('util');
var EventEmitter = require('events').EventEmitter;

const timeout = 2000;

var targets = {};

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
    var _this = this;
    EventEmitter.call(this);
    this.id = id;
    this.rate = 1; // msg/s for this target
    this.queue = [];
    this._resDelays = [];
    this._reqCount = 0;

    this.on('req', function () {
      _this._reqCount++;
    });

    this.on('res', function (date) {
      _this._resDelays.push(now() - date);
    });

    this.loop();
  }

  util.inherits(Target, EventEmitter);

  /**
   * Target internal loop : every seconds
   * - Compute the new rate
   * - Flush the queue util rate is reached
   */
  Target.prototype.loop = function () {
    var _this = this;

    var loopIt = 0;
    setInterval(function () {
      var lockedLoopIt = loopIt++;

      // compute the new rate every seconds
      var len = _this._resDelays.length;
      if (len === 0) return;
      var average = _.reduce(_this._resDelays, function (average, item) {
        return average + item / len;
      });
      _this.rate = Math.floor(len * 1000 / average);
      _this._resDelays = [];
      _this._reqCount = 0;

      // flush queue (check that current sent count < rate to avoid dequeue-enqueue)
      while (lockedLoopIt === loopIt && _this.isOpen()) {
        var msg = _this.queue.shift();
        exports.send(msg.from, msg.to, msg.content, msg.done);
      }
    }, 1000);
  };

  Target.prototype.isOpen = function () {
    return this._reqCount < this.rate;
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
    target.emit('req');
    var date = now();
    hubiquitus.send(from, to, content, timeout, function (err) {
      logger.trace('response from target', {target: target.id, err: err});
      if (err && err.code === 'TIMEOUT') {
        target.queue.push({from: from, to: to, content: content, done: done});
        target.emit('res', date);
      } else {
        done && done(err);
        if (!err) target.emit('res', date);
      }
    }, {safe: true});
  } else {
    logger.trace('target not opened, queue message', {target: target.id});
    target.queue.push({from: from, to: to, content: content, done: done});
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
  if (new Date().getTime() - req.date > req.timeout) {
    return logger.trace('timeout excedeed !', {req: req});
  }
  req.reply();
  next();
};

/**
 * Returns current timestamp
 * @returns {Number}
 */
function now() {
  return new Date().getTime();
}
