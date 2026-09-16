/*!
 * compression
 * Copyright(c) 2010 Sencha Inc.
 * Copyright(c) 2011 TJ Holowaychuk
 * Copyright(c) 2014 Jonathan Ong
 * Copyright(c) 2014-2015 Douglas Christopher Wilson
 * MIT Licensed
 */

'use strict'

/**
 * Module dependencies.
 * @private
 */

var Negotiator = require('negotiator')
var bytes = require('bytes')
var compressible = require('compressible')
var debug = require('node:util').debuglog('compression', fn => { debug = fn })
var onHeaders = require('on-headers')
var vary = require('vary')
var zlib = require('zlib')

/**
 * Module exports.
 */

module.exports = compression
module.exports.filter = shouldCompress

/**
 * Module variables.
 * @private
 */
var cacheControlNoTransformRegExp = /(?:^|,)\s*?no-transform\s*?(?:,|$)/i

const SUPPORTED_ENCODINGS = ['zstd', 'br', 'gzip', 'deflate', 'identity']

/**
 * Compress response data with gzip / deflate.
 *
 * @param {Object} [options]
 * @return {Function} middleware
 * @public
 */

function compression (options) {
  options = options || {}

  var optsZstd = { ...options.zstd }

  var optsBrotli = {
    ...options.brotli,
    params: {
      // set the default level to a reasonable value with balanced speed/ratio,
      // see https://blog.cloudflare.com/this-is-brotli-from-origin/#testing
      [zlib.constants.BROTLI_PARAM_QUALITY]: 4,
      ...options.brotli?.params
    }
  }

  // options
  var filter = options.filter || shouldCompress
  var threshold = bytes.parse(options.threshold) ?? 1024
  var enforceEncoding = options.enforceEncoding || 'identity'
  const preferredEncodings = options.preferredEncodings || ['br', 'zstd', 'gzip']

  return function compression (req, res, next) {
    var ended = false
    var length
    var listeners = []
    var stream
    var closed = false

    var _end = res.end
    var _on = res.on
    var _removeListener = res.removeListener
    var _write = res.write

    // flush
    res.flush = function flush () {
      if (stream) {
        stream.flush()
      }
    }

    // proxy

    res.write = function write (chunk, encoding) {
      if (ended) {
        return false
      }

      if (!headersSent(res)) {
        this.writeHead(this.statusCode)
      }

      return stream
        ? stream.write(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding))
        : _write.call(this, chunk, encoding)
    }

    res.end = function end (chunk, encoding) {
      if (ended) {
        return false
      }

      if (!headersSent(res)) {
        // estimate the length
        if (!this.getHeader('Content-Length')) {
          length = chunkLength(chunk, encoding)
        }

        this.writeHead(this.statusCode)
      }

      if (!stream) {
        return _end.call(this, chunk, encoding)
      }

      // mark ended
      ended = true

      // write Buffer for Node.js 0.8
      return chunk
        ? stream.end(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, encoding))
        : stream.end()
    }

    res.on = function on (type, listener) {
      if (!listeners || type !== 'drain') {
        return _on.call(this, type, listener)
      }

      if (stream) {
        return stream.on(type, listener)
      }

      // buffer listeners for future stream
      listeners.push([type, listener])

      return this
    }

    res.addListener = res.on

    res.removeListener = function removeListener (type, listener) {
      if (!listeners || type !== 'drain') {
        return _removeListener.call(this, type, listener)
      }

      if (stream) {
        return stream.removeListener(type, listener)
      }

      // remove buffered listener
      for (var i = listeners.length - 1; i >= 0; i--) {
        if (listeners[i][0] === type && listeners[i][1] === listener) {
          listeners.splice(i, 1)
        }
      }

      return this
    }

    if (res.off) {
      // emitter.off was added in Node.js v10+; don't add it to earlier versions
      res.off = res.removeListener
    }

    function nocompress (msg) {
      debug('no compression: %s', msg)
      addListeners(res, _on, listeners)
      listeners = null
    }

    // Release the compression stream when the response closes, freeing native
    // zlib resources even if the client disconnected before it finished.
    // Registered before onHeaders so a close before the stream exists is not
    // missed.
    _on.call(res, 'close', function onResponseClose () {
      closed = true
      if (stream) stream.destroy()
    })

    onHeaders(res, function onResponseHeaders () {
      // determine if request is filtered
      if (!filter(req, res)) {
        nocompress('filtered')
        return
      }

      // determine if the entity should be transformed
      if (!shouldTransform(req, res)) {
        nocompress('no transform')
        return
      }

      // vary
      vary(res, 'Accept-Encoding')

      // content-length below threshold
      if (Number(res.getHeader('Content-Length')) < threshold || length < threshold) {
        nocompress('size below threshold')
        return
      }

      var encoding = res.getHeader('Content-Encoding') || 'identity'

      // already encoded
      if (encoding !== 'identity') {
        nocompress('already encoded')
        return
      }

      // head
      if (req.method === 'HEAD') {
        nocompress('HEAD request')
        return
      }

      // compression method
      var negotiator = new Negotiator(req)
      var method = negotiator.encoding(SUPPORTED_ENCODINGS, preferredEncodings)

      // if no method is found, use the default encoding
      if (!req.headers['accept-encoding'] && SUPPORTED_ENCODINGS.includes(enforceEncoding)) {
        method = enforceEncoding
      }

      // negotiation failed
      if (!method || method === 'identity') {
        nocompress('not acceptable')
        return
      }

      // compression stream
      debug('%s compression', method)
      stream = method === 'gzip'
        ? zlib.createGzip(options)
        : method === 'br'
          ? zlib.createBrotliCompress(optsBrotli)
          : method === 'zstd'
            ? zlib.createZstdCompress(optsZstd)
            : zlib.createDeflate(options)

      // the response already closed before the stream was created, so the
      // close listener above has already run. Release the stream now and drop
      // the reference so later writes fall back to the raw response.
      if (closed) {
        stream.destroy()
        return
      }

      // add buffered listeners to stream
      addListeners(stream, stream.on, listeners)

      // header fields
      res.setHeader('Content-Encoding', method)
      res.removeHeader('Content-Length')

      // compression
      stream.on('data', function onStreamData (chunk) {
        if (_write.call(res, chunk) === false) {
          stream.pause()
        }
      })

      stream.on('end', function onStreamEnd () {
        _end.call(res)
      })

      _on.call(res, 'drain', function onResponseDrain () {
        stream.resume()
      })
    })

    next()
  }
}

/**
 * Add bufferred listeners to stream
 * @private
 */

function addListeners (stream, on, listeners) {
  for (var i = 0; i < listeners.length; i++) {
    on.apply(stream, listeners[i])
  }
}

/**
 * Get the length of a given chunk
 */

function chunkLength (chunk, encoding) {
  if (!chunk) {
    return 0
  }

  return Buffer.isBuffer(chunk)
    ? chunk.length
    : Buffer.byteLength(chunk, encoding)
}

/**
 * Default filter function.
 * @private
 */

function shouldCompress (req, res) {
  var type = res.getHeader('Content-Type')

  if (type === undefined || !compressible(type)) {
    debug('%s not compressible', type)
    return false
  }

  return true
}

/**
 * Determine if the entity should be transformed.
 * @private
 */

function shouldTransform (req, res) {
  var cacheControl = res.getHeader('Cache-Control')

  // Don't compress for Cache-Control: no-transform
  // https://tools.ietf.org/html/rfc7234#section-5.2.2.4
  return !cacheControl ||
    !cacheControlNoTransformRegExp.test(cacheControl)
}

/**
 * Determine if the response headers have been sent.
 *
 * @param {object} res
 * @returns {boolean}
 * @private
 */

function headersSent (res) {
  return typeof res.headersSent !== 'boolean'
    ? Boolean(res._header)
    : res.headersSent
}
