var h = require('hubiquitus-core');
h.logger.enable('hubiquitus:core:*');
var hqos = require(__dirname + '/../index');
var util = require('util');

h
  .use(hqos.middleware)
  .addActor('toto', function (req) {})
  .start({discoveryAddr: 'udp://224.0.0.1:5555'});
