var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'info');
var hubiquitusQos = require(__dirname + '/../../index');
var util = require('util');

hubiquitusQos.configure(function (err) {
  if (err) {
    return logger.err('qos addon configuration error');
  }

  function god(req) {
    console.log(req);
  };

  hubiquitus
    .use(hubiquitusQos.middleware)
    .addActor('god', god)
    .start({discoveryAddr: 'udp://224.0.0.1:5555'});

  hubiquitusQos.send('god', 'sampleActor', 'do safe work !', function (err) {
    if (err) {
      return logger.error('my message will not be processed :(' + err);
    }

    logger.info('ok, I have the warranty that my message will be processed');
  });
});
