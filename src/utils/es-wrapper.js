const { Client } = require('@elastic/elasticsearch')
const { createAWSConnection, awsGetCredentials } = require('@acuris/aws-es-connection')

module.exports = async (node, testMode, options) => {
  const esParams = { node }
  let AWSConnection = {}
  // Because we use ordinary elasticsearch container instead of AWS elasticsearch for integration tests
  // then if endpoint is localhost we cannot upload aws credentials
  if (!testMode && node.indexOf('localhost') === -1) {
    const awsCredentials = await awsGetCredentials()
    AWSConnection = createAWSConnection(awsCredentials)
  }

  const es = new Client({
    ...AWSConnection,
    ...esParams,
    ...options
  })

  return {
    index: ({ index, id, body, refresh }) => es.index({ index, id, body, refresh, timeout: '5m' }),
    remove: ({ index, id, refresh }) => es.delete({ index, id, refresh }),
    exists: ({ index, id, refresh }) => es.exists({ index, id, refresh }),
    indicesDelete: (index = '_all') => es.indices.delete({ index }),
    bulk: ({ refresh = true, body }) => es.bulk({ refresh, body })
  }
}
