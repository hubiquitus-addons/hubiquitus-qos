var h = require('hubiquitus-core');
var hqos = require(__dirname + '/../index');
var util = require('util');

function send(from, to, content) {
  hqos.send(from, to, content);
}

h
  .start({discoveryAddr: 'udp://224.0.0.1:5555'});

/*var series = [0, 25000, 0, 0, 0, 0, 5000, 9000, 0];
var len = series.length;
var index = -1;
setInterval(function () {
  var i = -1;
  index = ++index%len;
  while (++i < series[index]) {
    hqos.send('god', 'toto', 'hello');
  }
}, 100);*/

setInterval(function () {
  send('god', 'toto', 'hello');
}, 1);

setInterval(function () {
  var i = -1;
  while (++i < 15000) {
    send('god', 'toto', 'hello');
  }
}, 3000);

setInterval(function () {
  var i = -1;
  while (++i < 7500) {
    send('god', 'toto', 'hello');
  }
}, 1000);
