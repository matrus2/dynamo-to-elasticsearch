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

exports.pushStream = async (
  {
    event,
    index = getTableNameFromARN(event.Records[0].eventSourceARN),
    type = getTableNameFromARN(event.Records[0].eventSourceARN),
    endpoint,
    refresh = true,
    testMode = false,
    transformFunction = undefined,
    elasticSearchOptions
  } = {}) => {
  validateString(index, 'index')
  validateString(type, 'type')
  validateString(endpoint, 'endpoint')
  validateBoolean(refresh, 'refresh')
  validateFunctionOrUndefined(transformFunction, 'transformFunction')

  const es = await elastic(endpoint, testMode, elasticSearchOptions)

  const toRemove = []
  const toUpsert = []

  for (const record of event.Records) {
    const keys = converter(record.dynamodb.Keys)
    const id = Object.values(keys).reduce((acc, curr) => acc.concat(curr), '')

    switch (record.eventName) {
      case 'REMOVE': {
        toRemove.push({ index, type, id, refresh })
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
            toUpsert.push({ index, type, id, body, refresh })
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
    const bodyDelete = flatMap(toRemove, (doc) => [{ delete: { _index: doc.index, _id: doc.id } }])
    await es.bulk({ refresh: true, body: bodyDelete })
  }

  if (toUpsert.length > 0) {
    const updateBody = flatMap(toUpsert, (doc) => [
      { update: { _index: doc.index, _id: doc.id, _type: doc.type } },
      { doc: doc.body, doc_as_upsert: true }
    ])

    await es.bulk({ refresh: true, body: updateBody })
  }
}
