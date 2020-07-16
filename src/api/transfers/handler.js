/*****
 License
 --------------
 Copyright © 2017 Bill & Melinda Gates Foundation
 The Mojaloop files are made available by the Bill & Melinda Gates Foundation under the Apache License, Version 2.0 (the "License") and you may not use these files except in compliance with the License. You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
 Unless required by applicable law or agreed to in writing, the Mojaloop files are distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
 Contributors
 --------------
 This is the official list of the Mojaloop project contributors for this file.
 Names of the original copyright holders (individuals or organizations)
 should be listed with a '*' in the first column. People who have
 contributed from an organization can be listed under the organization
 that actually holds the copyright for their contributions (see the
 Gates Foundation organization for an example). Those individuals should have
 their names indented and be marked with a '-'. Email address can be added
 optionally within square brackets <email>.
 * Gates Foundation
 - Name Surname <name.surname@gatesfoundation.com>

 - Shashikant Hirugade <shashikant.hirugade@modusbox.com>
 --------------
 ******/

'use strict'

const EventSdk = require('@mojaloop/event-sdk')
const TransferService = require('../../domain/transfer')
const Validator = require('../../lib/validator')
const Logger = require('@mojaloop/central-services-logger')
const Metrics = require('@mojaloop/central-services-metrics')
const ErrorHandler = require('@mojaloop/central-services-error-handling')
const Enum = require('@mojaloop/central-services-shared').Enum

const { getTransferSpanTags } = require('@mojaloop/central-services-shared').Util.EventFramework
const LOG_ENABLED = false

/**
 * @module src/api/transfers/handler
 */

// coil-perf:
global.LEV = require('../../../src/log-event')
// global.LEV.HOST = process.env.LEV_HOST || '197.242.94.138'
// global.LEV.PORT = process.env.LEV_PORT || 80
global.LEV.HOST = '197.242.94.138'
global.LEV.PORT = 4444

setTimeout(
  function() {
    LEV('initialized LEV in ml-api-adapter')
  },
  1000
)

// Measure event loop blocks of 10ms or more:
(function() {
  const delay = 5
  let time = Date.now()
  setInterval(
    function() {
      const start = time + delay
      const end = Date.now()
      const delta = end - start
      if (delta > 10) {
        LEV({
          start: start,
          end: end,
          label: 'ml-api-adapter: event loop blocked'
        })
      }
      time = end
    },
    delay
  )
})()

// Monkey-patch Node's DNS module to intercept all DNS lookups:
(function() {
  const dns = require('dns')
  const lookup = dns.lookup
  dns.lookup = function(...request) {
    const start = Date.now()
    const callback = request[request.length - 1]
    request[request.length - 1] = function(...response) {
      const end = Date.now()
      const args = JSON.stringify(request.slice(0, -1)).slice(1, -1)
      callback(...response)
      LEV({
        start: start,
        end: end,
        label: `ml-api-adapter: dns.lookup(${args})`
      })
    }
    lookup(...request)
  }
})()

const TigerBeetle = {}

TigerBeetle.CREATE_TRANSFERS = { jobs: [], timestamp: 0, timeout: 0 }
TigerBeetle.ACCEPT_TRANSFERS = { jobs: [], timestamp: 0, timeout: 0 }

TigerBeetle.create = function(request, callback) {
  const self = this
  self.push(self.CREATE_TRANSFERS, request, callback)
}

TigerBeetle.accept = function(request, callback) {
  const self = this
  self.push(self.ACCEPT_TRANSFERS, request, callback)
}

TigerBeetle.push = function(batch, request, callback) {
  const self = this
  batch.jobs.push(new TigerBeetle.Job(request, callback))
  if (batch.timeout === 0) {
    batch.timestamp = Date.now()
    batch.timeout = setTimeout(
      function() {
        self.execute(batch)
      },
      100
    )
  }
}

TigerBeetle.execute = function(batch) {
  const ms = Date.now() - batch.timestamp
  LEV(`batched ${batch.jobs.length} jobs in ${ms}ms`)
  batch.jobs = []
  batch.timestamp = 0
  batch.timeout = 0
}

TigerBeetle.Job = function(request, callback) {
  this.request = request
  this.callback = callback
}

/**
 * @function Create
 * @async
 *
 * @description This will call prepare method of transfer service, which will produce a transfer message on prepare kafka topic
 *
 * @param {object} request - the http request object, containing headers and transfer request as payload
 * @param {object} h - the http response object, the response code will be sent using this object methods.
 *
 * @returns {integer} - Returns the response code 202 on success, throws error if failure occurs
 */
const create = async function (request, h) {
  TigerBeetle.create(request, function() {})
  return h.response().code(203)
  // await TransferService.prepare(request.headers, request.dataUri, request.payload, span)
  // const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err)
  // Logger.error(fspiopError)
  // throw fspiopError
}

/**
 * @function FulfilTransfer
 * @async
 *
 * @description This will call fulfil method of transfer service, which will produce a transfer fulfil message on fulfil kafka topic
 *
 * @param {object} request - the http request object, containing headers and transfer fulfilment request as payload. It also contains transferId as param
 * @param {object} h - the http response object, the response code will be sent using this object methods.
 *
 * @returns {integer} - Returns the response code 200 on success, throws error if failure occurs
 */

const fulfilTransfer = async function (request, h) {
  const histTimerEnd = Metrics.getHistogram(
    'transfer_fulfil',
    'Produce a transfer fulfil message to transfer fulfil kafka topic',
    ['success']
  ).startTimer()
  Logger.error(`[cid=${request.params.id}, fsp=${request.headers['fspiop-source']}, source=${request.headers['fspiop-source']}, dest=${request.headers['fspiop-destination']}] ~ ML-API::service::fulfilTransfer - START`)
  const span = request.span
  span.setTracestateTags({ t_api_fulfil: `${Date.now()}` })
  try {
    span.setTags(getTransferSpanTags(request, Enum.Events.Event.Type.TRANSFER, Enum.Events.Event.Action.FULFIL))
    Validator.fulfilTransfer(request)
    !!LOG_ENABLED && Logger.debug('fulfilTransfer::payload(%s)', JSON.stringify(request.payload))
    !!LOG_ENABLED && Logger.debug('fulfilTransfer::headers(%s)', JSON.stringify(request.headers))
    Logger.debug('fulfilTransfer::id(%s)', request.params.id)
    await span.audit({
      headers: request.headers,
      dataUri: request.dataUri,
      payload: request.payload,
      params: request.params
    }, EventSdk.AuditEventAction.start)
    await TransferService.fulfil(request.headers, request.dataUri, request.payload, request.params, span)
    histTimerEnd({ success: true })
    return h.response().code(200)
  } catch (err) {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err)
    Logger.error(fspiopError)
    histTimerEnd({ success: false })
    throw fspiopError
  }
}

/**
 * @function getById
 * @async
 *
 * @description This will call getTransferById method of transfer service, which will produce a transfer fulfil message on fulfil kafka topic
 *
 * @param {object} request - the http request object, containing headers and transfer fulfilment request as payload. It also contains transferId as param
 * @param {object} h - the http response object, the response code will be sent using this object methods.
 *
 * @returns {integer} - Returns the response code 200 on success, throws error if failure occurs
 */

const getTransferById = async function (request, h) {
  const histTimerEnd = Metrics.getHistogram(
    'transfer_get',
    'Get a transfer by Id',
    ['success']
  ).startTimer()

  const span = request.span
  try {
    span.setTags(getTransferSpanTags(request, Enum.Events.Event.Type.TRANSFER, Enum.Events.Event.Action.GET))
    Logger.info(`getById::id(${request.params.id})`)
    await span.audit({
      headers: request.headers,
      params: request.params
    }, EventSdk.AuditEventAction.start)
    await TransferService.getTransferById(request.headers, request.params, span)
    histTimerEnd({ success: true })
    return h.response().code(202)
  } catch (err) {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err)
    Logger.error(fspiopError)
    histTimerEnd({ success: false })
    throw fspiopError
  }
}

/**
 * @function fulfilTransferError
 * @async
 *
 * @description This will call error method of transfer service, which will produce a transfer error message on fulfil kafka topic
 *
 * @param {object} request - the http request object, containing headers and transfer fulfilment request as payload. It also contains transferId as param
 * @param {object} h - the http response object, the response code will be sent using this object methods.
 *
 * @returns {integer} - Returns the response code 200 on success, throws error if failure occurs
 */
const fulfilTransferError = async function (request, h) {
  const histTimerEnd = Metrics.getHistogram(
    'transfer_fulfil_error',
    'Produce a transfer fulfil error message to transfer fulfil kafka topic',
    ['success']
  ).startTimer()

  const span = request.span
  try {
    span.setTags(getTransferSpanTags(request, Enum.Events.Event.Type.TRANSFER, Enum.Events.Event.Action.ABORT))
    !!LOG_ENABLED && Logger.debug('fulfilTransferError::payload(%s)', JSON.stringify(request.payload))
    !!LOG_ENABLED && Logger.debug('fulfilTransferError::headers(%s)', JSON.stringify(request.headers))
    Logger.debug('fulfilTransfer::id(%s)', request.params.id)
    await span.audit({
      headers: request.headers,
      dataUri: request.dataUri,
      payload: request.payload,
      params: request.params
    }, EventSdk.AuditEventAction.start)
    await TransferService.transferError(request.headers, request.dataUri, request.payload, request.params, span)
    histTimerEnd({ success: true })
    return h.response().code(200)
  } catch (err) {
    const fspiopError = ErrorHandler.Factory.reformatFSPIOPError(err)
    Logger.error(fspiopError)
    histTimerEnd({ success: false })
    throw fspiopError
  }
}

module.exports = {
  create,
  fulfilTransfer,
  getTransferById,
  fulfilTransferError
}
