/* eslint-env mocha */
const { assert } = require('chai')
const fetch = require('node-fetch')
const AWS = require('aws-sdk')
const { pushStream } = require('./../src/index')
const elastic = require('../src/utils/es-wrapper')
const { sampleData, modifyEvent, removeEvent, insertEvent, multipleEvents, insertEventWithInconsitentTypes } = require('./fixtures')
const { removeEventData } = require('../src/utils/index')
const getTableNameFromARN = require('../src/utils/table-name-from-arn')

const converter = AWS.DynamoDB.Converter.unmarshall
const INDEX = 'test'
const ES_ENDPOINT = 'http://localhost:9200'

async function assertThrowsAsync (fn, regExp) {
  let f = () => {}
  try {
    await fn()
  } catch (e) {
    f = () => { throw e }
  } finally {
    assert.throws(f, regExp)
  }
}

// 1. run a docker container
// docker run -i -p 9200:9200 --name my_elastic -p 9300:9300 -e "discovery.type=single-node" elasticsearch:7.2.0
describe('Test stream events', () => {
  let es

  beforeEach(async () => {
    es = await elastic(ES_ENDPOINT, true)
    const promiseArray = sampleData.map(
      data => es.index({ index: INDEX, id: data.url, body: data }))
    await Promise.all(promiseArray).catch(e => { console.log(e) })
  })

  afterEach(async () => {
    await es.indicesDelete()
  })

  it('MODIFY: should modify existing item', async () => {
    await pushStream({ event: modifyEvent, index: INDEX, endpoint: ES_ENDPOINT, testMode: true })
    const keys = converter(modifyEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    const data = removeEventData(converter(modifyEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('REMOVE: should delete existing item', async () => {
    await pushStream({ event: removeEvent, index: INDEX, endpoint: ES_ENDPOINT, testMode: true })
    const keys = converter(removeEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isFalse(body.found)
  })

  it('REMOVE: should not delete if item doen\'t exists', async () => {
    const event = removeEvent
    event.Records[0].dynamodb.Keys.url.S = 'something-which-doesnt-exists'
    await pushStream({ event, index: INDEX, endpoint: ES_ENDPOINT, testMode: true })
    const keys = converter(removeEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isFalse(body.found)
  })

  it('INSERT: should insert new item', async () => {
    await pushStream({ event: insertEvent, index: INDEX, endpoint: ES_ENDPOINT, testMode: true })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(insertEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('INSERT: should insert new item when refresh false', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      refresh: false,
      testMode: true
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(insertEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('INSERT: should insert new item based on ARN values', async () => {
    await pushStream({ event: insertEvent, endpoint: ES_ENDPOINT, testMode: true })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const arnKey = getTableNameFromARN(insertEvent.Records[0].eventSourceARN)
    const result = await fetch(`${ES_ENDPOINT}/${arnKey}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(insertEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('INSERT: should insert new item with new field by transform function', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      transformFunction: insertFullAddress
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    assert.property(body._source, 'full_address')
  })

  it('INSERT: should insert new item with new field from transform promise func', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      transformFunction: transformPromise(true)
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    assert.property(body._source, 'foo')
    assert.equal(body._source.foo, 'bar')
  })

  it('INSERT: should insert new item with new field from transform promise func', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      transformFunction: transformPromiseTimeout(true)
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    assert.property(body._source, 'foo')
    assert.equal(body._source.foo, 'bar')
  })

  it('INSERT: should throw if promise rejected', async () => {
    const simpleData = {
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      transformFunction: transformPromise(false)
    }
    await assertThrowsAsync(async () => pushStream(simpleData), 'Your error')
  })

  it('INSERT: should insert new item without new field', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      transformFunction: undefined
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(insertEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('Multiple events: insert, remove, modify', async () => {
    await pushStream({ event: multipleEvents, index: INDEX, endpoint: ES_ENDPOINT, testMode: true })
    const removed = converter(multipleEvents.Records[2].dynamodb.Keys).url
    const inserted = converter(multipleEvents.Records[0].dynamodb.Keys).url
    const changed = converter(multipleEvents.Records[1].dynamodb.Keys).url
    // REMOVED
    let result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${removed}`)
    let body = await result.json()
    assert.isFalse(body.found)
    // INSERTED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    body = await result.json()
    assert.isTrue(body.found)
    // CHANGED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${changed}`)
    body = await result.json()
    const data = removeEventData(converter(multipleEvents.Records[1].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('Multiple events: insert, remove, modify using bulk option', async () => {
    await pushStream({ event: multipleEvents, index: INDEX, endpoint: ES_ENDPOINT, useBulk: true, testMode: true })
    const removed = converter(multipleEvents.Records[2].dynamodb.Keys).url
    const inserted = converter(multipleEvents.Records[0].dynamodb.Keys).url
    const changed = converter(multipleEvents.Records[1].dynamodb.Keys).url
    // REMOVED
    let result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${removed}`)
    let body = await result.json()
    assert.isFalse(body.found)
    // INSERTED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    body = await result.json()
    assert.isTrue(body.found)
    // CHANGED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${changed}`)
    body = await result.json()
    const data = removeEventData(converter(multipleEvents.Records[1].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('Throws errors when index not found when using bulk delete', async () => {
    await assertThrowsAsync(async () => pushStream({ event: removeEvent, index: 'test-no-exists', endpoint: ES_ENDPOINT, useBulk: true, testMode: true }), 'no such index [test-no-exists]')
  })

  it('Throws errors when index not found when using bulk insert with inconsistent document types', async () => {
    await assertThrowsAsync(async () => pushStream({ event: insertEventWithInconsitentTypes, index: INDEX, endpoint: ES_ENDPOINT, useBulk: true, testMode: true }), 'failed to parse field [addedDate] of type [long] in document with id \'kale-pasros-253b536b-1\'. Preview of field\'s value: \'undefined\'')
  })

  it('Multiple events: insert, remove, modify with refresh false', async () => {
    await pushStream({
      event: multipleEvents,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      refresh: false
    })
    const removed = converter(multipleEvents.Records[2].dynamodb.Keys).url
    const inserted = converter(multipleEvents.Records[0].dynamodb.Keys).url
    const changed = converter(multipleEvents.Records[1].dynamodb.Keys).url
    // REMOVED
    let result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${removed}`)
    let body = await result.json()
    assert.isFalse(body.found)
    // INSERTED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    body = await result.json()
    assert.isTrue(body.found)
    // CHANGED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${changed}`)
    body = await result.json()
    const data = removeEventData(converter(multipleEvents.Records[1].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('Multiple events: insert, remove, modify with refresh false using bulk', async () => {
    await pushStream({
      event: multipleEvents,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      testMode: true,
      refresh: false,
      useBulk: true
    })
    const removed = converter(multipleEvents.Records[2].dynamodb.Keys).url
    const inserted = converter(multipleEvents.Records[0].dynamodb.Keys).url
    const changed = converter(multipleEvents.Records[1].dynamodb.Keys).url
    // REMOVED
    let result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${removed}`)
    let body = await result.json()
    assert.isFalse(body.found)
    // INSERTED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    body = await result.json()
    assert.isTrue(body.found)
    // CHANGED
    result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${changed}`)
    body = await result.json()
    const data = removeEventData(converter(multipleEvents.Records[1].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })

  it('Input data: wrong index is not a string', async () => {
    const simpleData = {
      index: 12,
      body: { test: 'test', myvalue: { 1: 'test' } },
      id: '12',
      endpoint: ES_ENDPOINT,
      testMode: true
    }
    await assertThrowsAsync(async () => pushStream(simpleData), 'Please provide correct value for index')
  })

  it('Input data: wrong endpoint is not a string', async () => {
    const simpleData = {
      index: '12',
      body: { test: 'test', myvalue: { 1: 'test' } },
      id: '12',
      testMode: true
    }
    await assertThrowsAsync(async () => pushStream(simpleData), 'Please provide correct value for endpoint')
  })

  it('Input data: wrong refresh is not a boolean', async () => {
    const simpleData = {
      index: 'asd',
      body: { test: 'test', myvalue: { 1: 'test' } },
      id: '12',
      endpoint: ES_ENDPOINT,
      refresh: 'true',
      testMode: true
    }
    await assertThrowsAsync(async () => pushStream(simpleData), 'Please provide correct value for refresh')
  })

  it('Input data: wrong transform function', async () => {
    const simpleData = {
      index: 'asd',
      body: { test: 'test', myvalue: { 1: 'test' } },
      id: '12',
      endpoint: ES_ENDPOINT,
      refresh: true,
      testMode: true,
      transformFunction: null
    }
    await assertThrowsAsync(async () => pushStream(simpleData), 'Please provide correct value for transformFunction')
  })

  it('Input data: should skip index if body is false', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      transformFunction: (body) => false,
      testMode: true
    })
    const inserted = converter(insertEvent.Records[0].dynamodb.Keys).url
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    const body = await result.json()
    assert.isFalse(body.found)
  })

  it('Input data: should skip index if body is empty', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      transformFunction: body => {},
      testMode: true
    })
    const inserted = converter(insertEvent.Records[0].dynamodb.Keys).url
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    const body = await result.json()
    assert.isFalse(body.found)
  })

  it('Input data: should transform and insert based on action given oldImage', async () => {
    await pushStream({
      event: modifyEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      transformFunction: (body, oldBody) => {
        const { city } = oldBody
        return {
          ...body,
          city
        }
      },
      testMode: true
    })
    const inserted = converter(modifyEvent.Records[0].dynamodb.Keys).url
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${inserted}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(modifyEvent.Records[0].dynamodb.NewImage))
    const oldData = removeEventData(converter(modifyEvent.Records[0].dynamodb.OldImage))
    assert.deepEqual({
      ...data,
      city: oldData.city
    }, body._source)
  })

  it('Input data: can access record content in the transform function', async () => {
    await pushStream({
      event: modifyEvent,
      index: INDEX,

      endpoint: ES_ENDPOINT,
      transformFunction: (body, oldBody, record) => {
        const dynamoDbJSON = record.dynamodb.NewImage
        assert.isObject(dynamoDbJSON.city)
        assert.isString(dynamoDbJSON.city.S)
        return null
      },
      testMode: true
    })
  })

  it('INSERT: should insert new item based on elasticSearchOptions ', async () => {
    await pushStream({
      event: insertEvent,
      index: INDEX,

      endpoint: 'tobeoverwritten',
      testMode: true,
      transformFunction: undefined,
      elasticSearchOptions: { node: ES_ENDPOINT }
    })
    const keys = converter(insertEvent.Records[0].dynamodb.Keys)
    const result = await fetch(`${ES_ENDPOINT}/${INDEX}/_doc/${keys.url}`)
    const body = await result.json()
    assert.isTrue(body.found)
    const data = removeEventData(converter(insertEvent.Records[0].dynamodb.NewImage))
    assert.deepEqual(data, body._source)
  })
})

function insertFullAddress (record) {
  const hydratedRecord = JSON.parse(JSON.stringify(record))
  hydratedRecord.full_address = getFieldContent(record.city, '. ') + getFieldContent(record.country + '.')
  return hydratedRecord
}

const getFieldContent = (content, sep = ' ') => {
  return content ? content + sep : ''
}

const transformPromise = (resolved) => (record) => new Promise((resolve, reject) => {
  if (resolved) {
    resolve({ ...record, foo: 'bar' })
  }
  reject(new Error('Your error'))
})

const transformPromiseTimeout = (resolved) => (record) => new Promise((resolve, reject) => {
  setTimeout(() => {
    if (resolved) {
      resolve({ ...record, foo: 'bar' })
    }
    reject(new Error('Your error'))
  }, 1000)
})
