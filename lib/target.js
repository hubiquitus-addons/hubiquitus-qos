var util = require('util');
var EventEmitter = require('events').EventEmitter;
var properties = require('./properties');

exports.Target = Target;

function Target(id) {
  this.id = id;
  this.rate = 500;
  this.debug = false;
  this._queue = [];
  this._stats = {traffic: {}, queue: {}};
  this._initStats();
  this.processQueue = this._generateQueueProcessor();
  this.loop();
}

util.inherits(Target, EventEmitter);

Target.prototype._initStats = function () {
  this._stats.traffic.sent = 0;
  this._stats.traffic.ack = 0;
  this._stats.traffic.timeout = 0;
  this._stats.queue.in = 0;
  this._stats.queue.out = 0;
  this._stats.queue.scheduled = 0;
};

Target.prototype.ok = function () {
  return this._stats.traffic.sent < this.rate;
};

Target.prototype.notifyReq = function () {
  this._stats.traffic.sent++;
};

Target.prototype.notifyRes = function (delay) {
  if (delay >= properties.timeout) {
    this._stats.traffic.timeout++;
    this.rate = (this.rate - 1 < 0) ? 1 : this.rate - 1;
  } else {
    this._stats.traffic.ack++;
    this.rate++;
  }
};

Target.prototype.queue = function (item) {
  this._stats.queue.in++;
  this._queue.push(item);
};

Target.prototype._generateQueueProcessor = function () {
  var _this = this;
  var running = false;

  return function processQueue() {
    if (running) return; // already running, do nothing
    running = true;
    (function processItem() {
      if (_this._queue.length > 0 && _this.ok()) {
        _this._stats.queue.scheduled++;
        setImmediate(function () { // queue processing is not blocking
          var item = _this._queue.shift();
          _this._stats.queue.out++;
          _this.emit('processed', item);
          processItem();
        });
      } else {
        running = false;
      }
    })();
  };
};

Target.prototype.loop = function () {
  var _this = this;
  setInterval(function () {
    if (_this.debug) console.log(_this.toString());
    _this._initStats();
    _this.processQueue(); // restart queue processing if has been stopped
  }, 1000);
};

Target.prototype.toString = function () {
  var structure = 'Target %s - %d msg/s\n' +
    '\tTraffic: %d sent - %d ack - %d timeout\n' +
    '\tQueue: %d items - %d in - %d out - %d scheduled\n';
  return util.format(structure,
    this.id,
    this.rate,
    this._stats.traffic.sent,
    this._stats.traffic.ack,
    this._stats.traffic.timeout,
    this._queue.length,
    this._stats.queue.in,
    this._stats.queue.out,
    this._stats.queue.scheduled
  );
};
