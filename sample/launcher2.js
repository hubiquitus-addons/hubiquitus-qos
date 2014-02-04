var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'warn');
var hubiquitusQos = require(__dirname + '/../index');
var util = require('util');

hubiquitus
  .start({discoveryAddr: 'udp://224.0.0.1:5555'});

var i = 0;
while (i++ < 100000) {
  dosend();
}

/*setInterval(function () {
  dosend();
}, 1);*/

function dosend() {
  hubiquitusQos.send('god', 'sampleActor', 'do safe work !', function (err) {
    if (err) {
      return logger.error('Shit, my message will not be processed :(');
    }

    logger.info('Ok, I have the warranty that my message will be processed');
  });
}
