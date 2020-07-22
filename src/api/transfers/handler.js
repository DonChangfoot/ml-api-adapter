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
const event_loop_delay = 5
let event_loop_timestamp = Date.now()
setInterval(
  function() {
    const start = event_loop_timestamp + event_loop_delay
    const end = Date.now()
    const delta = end - start
    if (delta > 10) {
      LEV({
        start: start,
        end: end,
        label: `ml-api-adapter: event loop blocked for ${delta}ms`
      })
    }
    event_loop_timestamp = end
  },
  event_loop_delay
)

// Figure out where the event loop blocked:
// (be aware that this has a performance cost due to async-hooks)
// const event_loop_blocked_at = require('../../../src/blocked-at');
// event_loop_blocked_at(
//   function(ms, stack) {
//     LEV(`Blocked for ${ms}ms, operation started here: ${stack.join('\n')}`)
//   }
// )

// Monkey-patch Node's DNS module to intercept all DNS lookups:
const coil_perf_dns = require('dns')
const coil_perf_lookup = coil_perf_dns.lookup
coil_perf_dns.lookup = function(...request) {
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
  coil_perf_lookup(...request)
}

const TigerBeetle = require('../../../src/client')

const util = require('util')
const TBCreate = util.promisify(TigerBeetle.create)
const TBAccept = util.promisify(TigerBeetle.accept)

const TIGER_BEETLE_HOST = 'tb.perf.openafrica.network'
const TIGER_BEETLE_PORT = 30000

TigerBeetle.connect(TIGER_BEETLE_HOST, TIGER_BEETLE_PORT,
  function(error) {
    if (error) throw error
  }
)

// Test harness payee:
const PAYEE_HOST = '34.105.130.202'
const PAYEE_PORT = 3333

// Test harness payer:
const PAYER_HOST = '34.105.130.202'
const PAYER_PORT = 7777

const Node = { http: require('http'), process: process }

Node.process.on('uncaughtException',
  function(error) {
    LEV(`ml-api-adapter: UNCAUGHT EXCEPTION: ${error}`);
  }
);

function PostNotification(host, port, path, body, end) {
  LEV(`ml-api-adapter: notification path=${path}`);
  const headers = {
    'Content-Length': body.length
  }
  const options = {
    agent: ConnectionPool,
    method: 'POST',
    host: host,
    port: port,
    path: path,
    headers: headers
  }
  const request = Node.http.request(options,
    function(response) {
      const buffers = []
      response.on('data', function(buffer) { buffers.push(buffer) })
      response.on('end',
        function() {
          end()
        }
      )
    }
  )
  request.write(body)
  request.end()
}

// Create a keep-alive HTTP request connection pool:
// We don't want each and every notification to do a TCP handshake...
// This is critical. The lack of this causes multi-second event loop blocks.
const ConnectionPool = new Node.http.Agent({
  keepAlive: true,
  maxFreeSockets: 10000,
  timeout: 60 * 1000
})

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
  const source = Buffer.from(JSON.stringify(request.payload))
  const object = request.payload
  const target = TigerBeetle.encodeCreate(object)
  await TBCreate(target)
  const path = '/' + request.url.split('/').slice(3).join('/')
  PostNotification(PAYEE_HOST, PAYEE_PORT, path, source,
    function() {
    }
  )
  return h.response().code(202)
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
  const source = Buffer.from(JSON.stringify(request.payload))
  const object = request.payload
  const target = TigerBeetle.encodeAccept(object)
  await TBAccept(target)
  const path = '/' + request.url.split('/').slice(3).join('/')
  PostNotification(PAYER_HOST, PAYER_PORT, path, source,
    function() {
    }
  )
  return h.response().code(202)
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
