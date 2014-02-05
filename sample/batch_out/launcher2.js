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

  hubiquitusQos.manualQueue('god', 'sampleActor', 'do safe work !', function (err, res) {
    if (err) {
      return logger.error('manual queueing failed');
    }

    hubiquitusQos.send('god', 'sampleActor', 'do safe work !', function (err) {
      if (err) {
        return logger.error('my message will not be processed :(');
      }

      //res.done(); will be pinged and message will be resent to launcher1 actor
      logger.info('ok, I have the warranty that my message will be processed');
    });
  });
});
