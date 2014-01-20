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
    this.rate = 100; // msg/s for this target
    this._queue = [];
    this._resDelays = [];
    this._resAverageLatency = 0;
    this._reqCount = 0;
    this._unqueuedCount = 0;
    this._queuedCount = 0;

    this.on('req', function () {
      _this._reqCount++;
    });

    this.on('res', function (date) {
      _this._resDelays.push(date);
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

    var begin = new Date().getTime();
    var loopIt = 0;
    setInterval(function () {
      var end = new Date().getTime();
      console.log('last exec : ' + (end - begin));
      begin = end;
      var lockedLoopIt = ++loopIt;

      // compute the new rate every seconds

      var len = _this._resDelays.length;
      if (len !== 0) {
        var sum = _.reduce(_this._resDelays, function (sum, item) {
          return sum + item;
        });
        _this._resAverageLatency = sum / len;
        console.log(_this.toString());
        /*
         * 1 msg <=> A ms = A/1000 s (average)
         * X msg <=> 1 s  ==> x = 1/A/1000 = 1000/A
         * Asynchronous model => we have to take care of parallelism => x = msgCount * x
         * 3 msg : [50ms, 100ms, 150ms] => 1 msg : 100ms (average) but msg are processed in parallel => 3 msg ~ 100ms
         */
        _this.rate = Math.floor(len * 1000 / _this._resAverageLatency) || 1;
        _this._resDelays = [];
        _this._reqCount = 0;
        _this._unqueuedCount = 0;
        _this._queuedCount = 0;
      }

      // flush queue (check that current sent count < rate to avoid dequeue-enqueue)
      /*while (lockedLoopIt === loopIt && _this.isOpen() && _this._queue.length > 0) {
        _this._unqueuedCount++;
        var msg = _this._queue.shift();
        exports.send(msg.from, msg.to, msg.content, msg.done);
      }*/

      (function unqueue() {
        if (lockedLoopIt === loopIt && _this.isOpen() && _this._queue.length > 0) {
          setImmediate(function () {
            _this._unqueuedCount++;
            var msg = _this._queue.shift();
            exports.send(msg.from, msg.to, msg.content, msg.done);
            unqueue();
          });
        }
      })();
    }, 1000);
  };

  Target.prototype.queue = function (item) {
    this._queuedCount++;
    this._queue.push(item);
  };

  Target.prototype.isOpen = function () {
    return this._reqCount < this.rate;
  };

  Target.prototype.toString = function () {
    return this.id + '> rate : ' + this.rate + ' msg/s\n' +
      this._reqCount + ' items sent (' + this._unqueuedCount + ' from queue)\n' +
      this._resDelays.length + ' ack|timeout received (average : ' + this._resAverageLatency + ' ms)\n' +
      this._queue.length + ' items in queue (' + this._queuedCount + ' new)\n';
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
      var time = now() - date;
      logger.trace('response from target', {target: target.id, err: err});
      if (err && err.code === 'TIMEOUT') {
        target.queue({from: from, to: to, content: content, done: done});
        target.emit('res', date);
      } else {
        done && done(err);
        if (!err) target.emit('res', time);
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
