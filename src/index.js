const AWS = require('aws-sdk')
const flatMap = require('lodash.flatmap')
const converter = AWS.DynamoDB.Converter.unmarshall
const elastic = require('./utils/es-wrapper')
const getTableNameFromARN = require('./utils/table-name-from-arn')
const { removeEventData } = require('./utils/index')

const validateString = (param, paramName) => {
  if (!param || !(typeof param === 'string')) throw new Error(`Please provide correct value for ${paramName}`)
}
const validateBoolean = (param, paramName) => {
  if (!(typeof param === 'boolean')) throw new Error(`Please provide correct value for ${paramName}`)
}
const validateFunctionOrUndefined = (param, paramName) => {
  if (!(typeof param === 'undefined' || typeof param === 'function')) throw new Error(`Please provide correct value for ${paramName}`)
}

const handleBulkResponseErrors = (bulkResponse, body) => {
  if (bulkResponse.errors) {
    const erroredDocuments = []
    bulkResponse.items.forEach((action, i) => {
      const operation = Object.keys(action)[0]
      if (action[operation].error) {
        erroredDocuments.push({
          status: action[operation].status,
          error: action[operation].error,
          operation: body[i * 2],
          document: body[i * 2 + 1]
        })
      }
    })
    console.error(erroredDocuments)
    throw new Error(erroredDocuments.map(obj => obj.error.reason).join(','))
  }
}

exports.pushStream = async (
  {
    event,
    index = getTableNameFromARN(event.Records[0].eventSourceARN),
    endpoint,
    refresh = true,
    testMode = false,
    useBulk = false,
    transformFunction = undefined,
    elasticSearchOptions
  } = {}) => {
  validateString(index, 'index')
  validateString(endpoint, 'endpoint')
  validateBoolean(refresh, 'refresh')
  validateBoolean(useBulk, 'useBulk')
  validateFunctionOrUndefined(transformFunction, 'transformFunction')

  const es = await elastic(endpoint, testMode, elasticSearchOptions)

  const toRemove = []
  const toUpsert = []

  for (const record of event.Records) {
    const keys = converter(record.dynamodb.Keys)
    const id = Object.values(keys).reduce((acc, curr) => acc.concat(curr), '')

    switch (record.eventName) {
      case 'REMOVE': {
        toRemove.push({ index, id, refresh })
        break
      }
      case 'MODIFY':
      case 'INSERT': {
        let body = converter(record.dynamodb.NewImage)
        const oldBody = record.dynamodb.OldImage ? converter(record.dynamodb.OldImage) : undefined
        body = removeEventData(body)
        if (transformFunction) {
          body = await Promise.resolve(transformFunction(body, oldBody, record))
        }
        try {
          if (
            body &&
            (Object.keys(body).length !== 0 && body.constructor === Object)
          ) {
            toUpsert.push({ index, id, body, refresh })
          }
        } catch (e) {
          throw new Error(e)
        }
        break
      }
      default:
        throw new Error(record.eventName + ' wasn\'t recognized')
    }
  }

  if (toRemove.length > 0) {
    if (useBulk === true) {
      const bodyDelete = flatMap(toRemove, (doc) => [{ delete: { _index: doc.index, _id: doc.id } }])
      const { body: bulkResponse } = await es.bulk({ refresh: toRemove[0].refresh, body: bodyDelete })
      if (bulkResponse.errors) {
        handleBulkResponseErrors(bulkResponse, bodyDelete)
      }
    } else {
      for (const doc of toRemove) {
        const { index, id, refresh } = doc
        const { body: exists } = await es.exists({ index, id, refresh })
        if (exists) {
          await es.remove({ index, id, refresh })
        }
      }
    }
  }

  if (toUpsert.length > 0) {
    if (useBulk === true) {
      const updateBody = flatMap(toUpsert, (doc) => [
        { update: { _index: doc.index, _id: doc.id } },
        { doc: doc.body, doc_as_upsert: true }
      ])
      const { body: bulkResponse } = await es.bulk({ refresh: toUpsert[0].refresh, body: updateBody })
      if (bulkResponse.errors) {
        handleBulkResponseErrors(bulkResponse, updateBody)
      }
    } else {
      for (const doc of toUpsert) {
        const { index, id, body, refresh } = doc
        await es.index({ index, id, body, refresh })
      }
    }
  }
}
