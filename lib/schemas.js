exports.conf = {
  title: 'configuration',
  description: 'hubiquitus-qos addon configuration',
  type: 'object',
  properties: {
    timeout: {type: 'integer'},
    processingTimeout: {type: 'integer'},
    mongo: {
      type: 'object',
      properties: {
        host: {type: 'string'},
        port: {type: 'integer'},
        dbname: {type: 'string'},
        collection: {type: 'string'}
      },
      required: [],
      additionalProperties: false
    }
  },
  required: [],
  additionalProperties: false
};

exports.batchConf = {
  title: 'batch-configuration',
  description: 'hubiquitus-qos addon batch configuration',
  type: 'object',
  properties: {
    gcInterval: {type: 'integer'},
    timeout: {type: 'integer'},
    limit: {type: 'integer'},
    mongo: {
      type: 'object',
      properties: {
        host: {type: 'string'},
        port: {type: 'integer'},
        dbname: {type: 'string'},
        collection: {type: 'string'}
      },
      required: [],
      additionalProperties: false
    }
  },
  required: [],
  additionalProperties: false
};
