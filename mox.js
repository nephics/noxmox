
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


function fakeReadableStream() {
  var self = new events.EventEmitter();

  self.readable = true;
  self.setEncoding = self.pause = self.resume = self.pipe = function() {};
  self.destroy = self.destroySoon = function() { self.readable = false; }

  return self;
}

function wrapReadableStream(rs) {
  var self = new events.EventEmitter();

  self.readable = true;
  self.setEncoding = rs.setEncoding;
  self.pause = rs.pause;
  self.resume = rs.resume;
  self.destroy = function() { self.readable = false; rs.destroy(); };
  self.destroySoon = function() { self.readable = false; rs.destroySoon(); };
  self.pipe = rs.pipe;
  
  rs.on('data', function(chunk) { self.emit('data', chunk); });
  rs.on('end', function() { self.readable = false; self.emit('end'); });
  rs.on('error', function(err) { self.readable = false; self.emit('error', err); });
  rs.on('close', function() { self.emit('close'); });

  return self;
}

function fakeWritableStream() {
  var self = new events.EventEmitter();
  
  self.writable = true;
  self.write = function(chunk, enc) { return true; };
  self.end = self.destroy = self.destroySoon = function() { self.writable = false; };
  
  return self;
}

function wrapWritableStream(ws) {
  var self = new events.EventEmitter();

  self.writable = true;
  self.write = function(chunk, enc) { return ws.write(chunk, enc); };
  self.end = function(chunk, enc) { self.writable = false; if (chunk) ws.end(chunk, enc); };
  self.destroy = function() { self.writable = false; ws.destroy(); }
  self.destroySoon = function() { self.writable = false; ws.destroySoon(); };

  ws.on('drain', function() { self.emit('drain'); });
  ws.on('error', function(err) { self.emit('error', err); });
  ws.on('close', function() { self.emit('close'); });
  ws.on('pipe', function(src) { self.emit('pipe', src); });

  return self;
}

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
        fs.mkdirSync(p, 0777);
      }
    }
    return filePath;
  }

  
  var client = new function() {};

  
  client.put = function put(filename, headers) {
    var filePath = getFilePath(filename, true);
    var fileLength = 0;
    var md5 = crypto.createHash('md5');
    // TODO handle copy and meta directives
    
    // create file stream to write the file data
    var ws = fs.createWriteStream(filePath);
    var request = wrapWritableStream(ws);
    ws.on('open', function() { request.emit('continue'); });

    // wrap request.write() to allow calculation of MD5 hash
    request._write = request.write;
    request.write = function write(chunk, enc) {
      fileLength += chunk.length;
      md5.update(chunk);
      return request._write(chunk, enc);
    };

    // wrap request.end() to write meta-data file and emit response
    request._end = request.end;
    request.end = function end(chunk, enc) {
      request._end(chunk, enc);

      // write the meta data file
      headers['content-length'] = fileLength;
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
        var response = fakeReadableStream();
        response.httpVersion = '1.1';
        response.statusCode = 200;
        response.headers = {
          etag:headers.ETag,
          date:headers.Date,
          'content-length':'0',
          server:'Mox'
        };
        request.emit('response', response);
        response.emit('end');
        response.readable = false;
        response.emit('close');
      });
    };

    request.abort = request.destroy;

    return request;
  };
  

  client.get = function get(filename, headers) {
    var request = fakeWritableStream();
    var filePath = getFilePath(filename);
    
    // read meta data
    fs.readFile(filePath + '.meta', 'utf8', function(err, data) {
      if (err) {
        request.writable = false;
        request.emit('error', err);
        return;
      }
      
      // create file stream for reading the requested file
      var rs = fs.createReadStream(filePath);
      var response = wrapReadableStream(rs);
      response.httpVersion = '1.1';
      response.statusCode = 200;
      var headers = JSON.parse(data);
      headers['last-modified'] = headers['date'];
      headers['date'] = (new Date()).toUTCString();
      headers['server'] = 'Mox';
      response.headers = headers;

      rs.on('open', function() {
        // file is ready, emit response
        request.emit('response', response);
      });
      rs.on('error', function(ex) {
        // emit file read error on the request
        request.writable = false;
        request.emit('error', ex);
      });
    });

    request.abort = request.destroy;

    return request;
  };

  
  client.head = function(filename, headers) {
    var request = fakeWritableStream();
    var filePath = getFilePath(filename);

    // read meta data
    fs.readFile(filePath + '.meta', 'utf8', function(err, data) {
      if (err) {
        request.writable = false;
        request.emit('error', err);
        return;
      }

      var response = fakeReadableStream();
      response.httpVersion = '1.1';
      response.statusCode = 200;
      meta = JSON.parse(data);
      meta['last-modified'] = meta['date'];
      meta['date'] = (new Date()).toUTCString();
      meta['server'] = 'Mox';
      response.headers = meta;

      request.emit('response', response);
      response.emit('end');
      response.emit('close');
    });

    request.abort = request.destroy;

    return request;
  };
  

  client.del = function del(filename) {
    var request = fakeWritableStream();
    var filePath = getFilePath(filename);
    
    // remove the file
    fs.unlink(filePath, function(err) {
      if (err) {
        request.writable = false;
        request.emit('error', err);
        return;
      }
      // remove meta data file
      fs.unlink(filePath + '.meta', function(err) {
        if (err) {
          request.writable = false;
          request.emit('error', err);
          return;
        }
        // when files are deleted, emit the response
        var response = fakeReadableStream();
        response.httpVersion = '1.1';
        response.statusCode = 204;
        response.headers = {
          date:(new Date()).toUTCString(),
          'content-length':'0',
          server:'Mox',
          connection:'close'
        };
        request.emit('response', response);
        response.emit('end');
        response.emit('close');
      });
    });

    request.abort = request.destroy;

    return request;
  };

  return client;
};
