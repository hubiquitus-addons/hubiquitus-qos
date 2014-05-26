var mongo = require('mongodb');

exports.timeout = 5000;
exports.processingTimeout = 10000;

exports.mongo = 'mongodb://localhost:27017/qos';
exports.collection = 'queue';

exports.batch = {
  gcInterval: 1000,
  gcTimeout: 30000,
  gcLimit: 10000,
  gcReplayTimeout: 10000,
  gcRetryLimit: 5,
  mongo: 'mongodb://localhost:27017/qos',
  collection: 'queue'
};
