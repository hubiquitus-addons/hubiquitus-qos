var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:core:*');
hubiquitus.logger.enable('hubiquitus:addons:*', 'trace');
var hubiquitusQos = require('./index');

hubiquitus
  .use(hubiquitusQos.middleware)
  .start()
  .addActor('toto', function (req) {
    logger.info(this.id + '> from ' + req.from + ' : ' + req.content);
  });

hubiquitusQos.send('god', 'toto', 'Safe hello !', function (err) {
  if (err) {
    logger.warn('error sending safe message', err);
  } else {
    logger.info('ACK from message sent safely !');
  }
});
