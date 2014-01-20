var h = require('hubiquitus-core');
var hqos = require(__dirname + '/../index');
var util = require('util');

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
  hqos.send('god', 'toto', 'hello');
}, 1);

setTimeout(function () {
  var i = -1;
  while (++i < 10000) {
    hqos.send('god', 'toto', 'hello');
  }
}, 5000);
