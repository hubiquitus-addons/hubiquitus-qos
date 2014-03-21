var mongo = require('mongodb');

exports.timeout = 5000;
exports.processingTimeout = 10000;

exports.mongo = {
  host: '127.0.0.1',
  port: mongo.Connection.DEFAULT_PORT,
  dbname: 'qos',
  collection: 'queue'
};

exports.batch = {
  gcInterval: 1000,
  gcTimeout: 30000,
  gcLimit: 10000,
  gcReplayTimeout: 10000,
  mongo: {
    host: '127.0.0.1',
    port: mongo.Connection.DEFAULT_PORT,
    dbname: 'qos',
    collection: 'queue'
  }
};
