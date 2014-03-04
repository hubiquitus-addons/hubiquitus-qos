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
  .option('--discovery [str]', 'Discovery addr')
  .option('--mongo-host [str]', 'Mongo host')
  .option('--mongo-port [n]', 'Mongo port', parseInt)
  .option('--mongo-dbname [str]', 'Mongo dbname')
  .option('--mongo-collection [str]', 'Mongo collection')
  .parse(process.argv);

hubiquitus.start({discoveryAddr: 'udp://224.0.0.1:5555'});
hubiquitusQos.batch.run();
