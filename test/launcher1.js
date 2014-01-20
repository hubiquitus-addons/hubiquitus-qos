var h = require('hubiquitus-core');
var hqos = require(__dirname + '/../index');
var util = require('util');

h
  .use(hqos.middleware)
  .addActor('toto', function () {
    console.log('.');
  })
  .start({discoveryAddr: 'udp://224.0.0.1:5555'});
