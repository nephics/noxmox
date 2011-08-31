
// mox - S3 mock-up for node.js
//
// Copyright(c) 2011 Nephics AB
// MIT Licensed
//

var crypto = require('crypto');
var fs = require('fs');
var events = require('events');
var path = require('path');
var util = require('util');

exports.createClient = function createClient(options) {
  if (!options.bucket) throw new Error('aws "bucket" required');
  if (!options.prefix) {
    options.prefix = '/tmp/mox';
  }
  // create storage dir, if it doesn't exists
  if (!path.existsSync(options.prefix)) {
    fs.mkdirSync(options.prefix, 0777);
  }
  // create bucket dir, if it does not exists
  var bucketPath = path.join(options.prefix, options.bucket);
  if (!path.existsSync(bucketPath)) {
    fs.mkdirSync(bucketPath, 0777);
  }

  function getFilePath(filename, createPath) {
    var filePath = path.join(bucketPath, filename);
    if (createPath) {
      // ensure that the path to the file exists
      createRecursive(path.dirname(filePath));
      function createRecursive(p) {
        if (path.existsSync(p) || p === bucketPath) return;
        createRecursive(path.join(p, '..'));
        console.log('Creating path: ' + p);
        fs.mkdirSync(p, 0777);
      }
    }
    return filePath;
  }

  var client = new function() {};

  client.put = function put(filename, headers) {
    var request = new events.EventEmitter();
    var filePath = getFilePath(filename, true);
    var fileLength = 0;
    var md5 = crypto.createHash('md5');
    // TODO handle copy and meta directives

    // create file stream to write the file data
    var ws = fs.createWriteStream(filePath);
    ws.on('error', function(ex) {
      request.emit('error', ex);
    });

    ws.on('open', function() {
      request.emit('continue');
    });

    request.write = function write(chunk, enc) {
      ws.write(chunk, enc);
      fileLength += chunk.length;
      md5.update(chunk);
    }

    request.end = function(chunk, enc) {
      if (chunk) request.write(chunk, enc);
      ws.destroySoon();
      // write the meta data file
      headers['Content-Length'] = fileLength;
      headers.Date = (new Date()).toUTCString();
      headers.ETag = '"' + md5.digest('hex') + '"';

      var meta = {};
      Object.keys(headers).forEach(function(key) {
        meta[key.toLowerCase()] = headers[key];
      });

      fs.writeFile(filePath + '.meta', JSON.stringify(meta), 'utf8', function(err) {
        if (err) {
          request.emit('error', err);
          return;
        }
        // when all data is written, the response is emitted
        var response = new events.EventEmitter();
        response.statusCode = 200;
        response.headers = {
          etag:headers.ETag,
          date:headers.Date,
          'content-length':'0',
          server:'Mox'
        };
        request.emit('response', response);
        response.emit('end');
      });
    }

    return request;
  };

  client.get = function get(filename, headers) {
    var request = new events.EventEmitter();
    var filePath = getFilePath(filename);

    // read meta data
    fs.readFile(filePath + '.meta', 'utf8', function(err, data) {
      if (err) {
        request.emit('error', err);
        return;
      }

      var response = new events.EventEmitter();
      response.statusCode = 200;
      var headers = JSON.parse(data);
      headers['last-modified'] = headers['date'];
      headers['date'] = (new Date()).toUTCString();
      headers['server'] = 'Mox';
      response.headers = headers;

      // create file stream for reading the requested file
      var ws = fs.createReadStream(filePath);
      ws.on('open', function() {
        request.emit('response', response);
      });
      ws.on('error', function(ex) {
        request.emit('error', ex);
      });
      ws.on('data', function(chunk) {
        response.emit('data', chunk);
      });
      ws.on('end', function() {
        response.emit('end');
      });
    });

    request.write = request.end = function () {};

    return request;
  };

  client.head = function(filename, headers) {
    var request = new events.EventEmitter();
    var filePath = getFilePath(filename);

    // read meta data
    fs.readFile(filePath + '.meta', 'utf8', function(err, data) {
      if (err) {
        request.emit('error', err);
        return;
      }

      var response = new events.EventEmitter();
      response.statusCode = 200;
      meta = JSON.parse(data);
      meta['last-modified'] = meta['date'];
      meta['date'] = (new Date()).toUTCString();
      meta['server'] = 'Mox';
      response.headers = meta;

      request.emit('response', response);
      response.emit('end');
    });

    request.write = request.end = function () {};

    return request;
  };

  client.del = function del(filename) {
    var request = new events.EventEmitter();
    var filePath = getFilePath(filename);
    // remove the file
    fs.unlink(filePath, function(err) {
      if (err) {
        request.emit('error', 'Could not remove file: ' + err.message);
        return;
      }
      // remove meta data file
      fs.unlink(filePath + '.meta', function(err) {
        if (err) {
          request.emit('error', 'Could not remove meta data file: ' + err.message);
          return;
        }
        // when files are deleted, emit the response
        var response = new events.EventEmitter();
        response.statusCode = 204;
        response.headers = {
          date:(new Date()).toUTCString(),
          'content-length':'0',
          server:'Mox',
          connection:'close'
        };
        request.emit('response', response);
        response.emit('data', 'File removed from disk');
        response.emit('end');
      });
    });

    request.write = request.end = function () {};
    return request;
  };

  return client;
};
