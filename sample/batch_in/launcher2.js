var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'trace');
var hubiquitusQos = require(__dirname + '/../../index');
var util = require('util');

hubiquitus
  .start({discoveryAddr: 'udp://224.0.0.1:5555'});

hubiquitusQos.configure({debug: true}, function (err) {
  if (err) {
    return logger.err('qos addon configuration error');
  }

  hubiquitusQos.send('god', 'sampleActor', 'do safe work !', function (err) {
    if (err) {
      return logger.error('my message will not be processed :(');
    }

    logger.info('ok, I have the warranty that my message will be processed');
  });
});
