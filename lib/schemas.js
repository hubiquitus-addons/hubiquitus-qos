exports.conf = {
  title: 'configuration',
  description: 'hubiquitus-qos addonc configuration',
  type: 'object',
  properties: {
    timeout: {type: 'integer'},
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