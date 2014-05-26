var hubiquitus = require('hubiquitus-core');
var commander = require('commander');
var version = require(__dirname + '/../package').version;
var hubiquitusQos = require(__dirname + '/../index');

hubiquitus.logger.enable('hubiquitus:addons:qos:batch', 'trace');

commander
  .version(version)
  .option('-d, --debug', 'Debug')
  .option('-g, --gc-interval [n]', 'GC interval', parseInt)
  .option('-t, --timeout [n]', 'Processing timeout', parseInt)
  .option('-l, --limit [n]', 'Items processed per tick', parseInt)
  .option('--discovery [str]', 'Discovery addr')
  .option('--mongo [str]', 'Mongo uri [mongodb://localhost:27017/qos]')
  .option('--collection [str]', 'Mongo collection [queue]')
  .parse(process.argv);

hubiquitus.start({discoveryAddr: 'udp://224.0.0.1:5555'});
hubiquitusQos.batch.run({
  gcInterval: 15000,
  gcTimeout: 10000
}, function (err) {
  if (err) process.exit(1);
});
