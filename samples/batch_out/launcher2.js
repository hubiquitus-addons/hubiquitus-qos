var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'trace');
var hubiquitusQos = require(__dirname + '/../../index');
var util = require('util');

hubiquitusQos.configure(function (err) {
  if (err) {
    return logger.err('qos addon configuration error');
  }

  hubiquitus
    .use(hubiquitusQos.middleware)
    .addActor('god/1', function () {})
    .start({discoveryAddr: 'udp://224.0.0.1:5555'});

  hubiquitusQos.persistAndSend('god/1', 'sampleActor', 'do safe work !', {userInfos:{name:'hubi'}}, function (err) {
    if (err) {
      return logger.warn('error while sending message safely', err);
    }
    logger.info('ok, I have the warranty that my message will be processed');
  });
});

