var util = require('util');
var _ = require('lodash');
var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'info');
var hubiquitusQos = require(__dirname + '/../../index');

hubiquitusQos.configure(function (err) {
  if (err) {
    return logger.err('qos addon configuration error');
  }

  hubiquitus
    .use(hubiquitusQos.middleware)
    .addActor('sampleActor', sampleActor)
    .start({discoveryAddr: 'udp://224.0.0.1:5555'});
});

function sampleActor(req) {
  logger.info('received safe message');
  req.reply();
}
