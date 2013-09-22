
//
// Copyright(c) 2011-2012 Nephics AB
// MIT Licensed
//


var fs = require('fs');
var crypto = require('crypto');
var util = require('util');
var assert = require('assert');

var nox = require('./nox.js');
var mox = require('./mox.js');


// Read aws auth data from stdin or cmd line argument, and run the tests
(function readAuthData() {
  var data = [];
  var timeout;
  
  if (process.argv.length >= 3) {
    // read from filename given as argument

    fs.readFile(process.argv[2], 'utf8', function(err, data) {
      if (err) {
        console.log('Failed to read file ' + process.argv[2] + ', error: ' +
            err);
      }
      else runTests(data);
    });
  }
  else {
    // try to read from stdin, bail out if no data after 100ms
    
    timeout = setTimeout(function() {
      if (!data.length) failed();
    }, 100);
    
    process.stdin.resume();
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', function (chunk) {
      data.push(chunk);
    });
    process.stdin.on('end', function () {
      runTests(data.join(''));
    });
    process.stdin.on('error', failed);
  }
  
  function failed() {
    console.log(['To run the tests you will need to supply JSON ',
      'encoded object with the aws key, secret and bucketname.\n',
      'Example calls:\n\n',
      '   cat aws_auth.json | ', process.argv[0], ' ', process.argv[1], '\n\n',
      '   ', process.argv[0], ' ', process.argv[1], ' aws_auth.json\n']
      .join(''));
    process.exit(1);
  }
}());


function runTests(data) {
  var options;
  try {
    options = JSON.parse(data);
  }
  catch (e) {
    console.log('Failed to parse input as JSON data');
    process.exit(1);
  }
  console.log('\nTesting mox client');
  var moxclient = mox.createClient(options);
  test(moxclient, function() {
    var noxclient = nox.createClient(options);
    console.log('\nTesting nox client');
    test(noxclient, function(){
      console.log('\nAll tests completed');
    });
  });
}


function test(client, callback) {
  var name = 'test/noxmox.txt';
  t1();
  function t1() {
    var buf = new Buffer('Testing the noxmox lib.');
    upload(client, name, buf, t2);
  }
  function t2() {
    stat(client, name, t3);
  }
  function t3() {
    download(client, name, t4);
  }
  function t4() {
    remove(client, name, t5);
  }
  function t5() {
    statRemoved(client, name, t6);
  }
  function t6() {
    downloadRemoved(client, name, t7);
  }
  function t7() {
    removeRemoved(client, name, callback);
  }
}

function logErrors(req) {
  req.on('error', function(err) {
    console.log(err.message || err);
  });
}

function logResponse(res) {
  console.log('status code: ' + res.statusCode);
  console.log('headers: ' + util.inspect(res.headers));
}


function upload(client, name, buf, callback) {
  console.log('\nFile upload');
  var req = client.put(name, {
    'Content-Type':'text/plain',
    'Content-Length':buf.length,
    'Content-MD5': crypto.createHash('md5').update(buf).digest('base64')
  });
  logErrors(req);
  req.on('continue', function() {
    req.end(buf);
  });
  req.on('response', function(res) {
    logResponse(res);
    res.on('data', function(chunk) {
      console.log(chunk);
    });
    res.on('end', function() {
      console.log('Response finished');
      if (res.statusCode === 404) {
        console.log('Failed to upload file, make sure the test bucket exists!');
        process.exit(2);
      }
      if (res.statusCode === 307) {
        console.log('Failed to upload file to bucket in non-standard region, make sure to include the endpoint in the bucket name: ' +
            res.headers.location.match(/\/\/(.*\.amazonaws\.com).*/)[1]);
        process.exit(3);
      }
      assert.equal(res.statusCode, 200);
      callback();
    });
  });
}


function stat(client, name, callback) {
  var req = client.head(name);
  logErrors(req);
  console.log('\nFile stat');
  req.on('response', function(res) {
    logResponse(res);
    res.on('data', function(chunk) {
      console.log(chunk);
    });
    res.on('end', function() {
      console.log('Response finished');
      assert.equal(res.statusCode, 200);
      callback();
    });
  });
  req.end();
}


function statRemoved(client, name, callback) {
  var req = client.head(name);
  logErrors(req);
  console.log('\nNonexistent file stat');
  req.on('response', function(res) {
    logResponse(res);
    res.on('data', function(chunk) {
      console.log(chunk);
    });
    res.on('end', function() {
      console.log('Response finished');
      assert.equal(res.statusCode, 404);
      callback();
    });
  });
  req.end();
}


function download(client, name, callback) {
  var req = client.get(name);
  logErrors(req);
  console.log('\nFile download');
  req.on('response', function(res) {
    logResponse(res);
    var len = 0;
    res.on('data', function(chunk) {
      len += chunk.length;
    });
    res.on('end', function() {
      console.log('Downloaded ' + len + ' bytes of file data');
      assert.equal(res.statusCode, 200);
      callback();
    });
  });
  req.end();
}

function downloadRemoved(client, name, callback) {
  var req = client.get(name);
  logErrors(req);
  console.log('\nNonexistent file download');
  req.on('response', function(res) {
    logResponse(res);
    res.on('data', function(chunk) {
      console.log(chunk.toString());
    });
    res.on('end', function() {
      assert.equal(res.statusCode, 404);
      callback();
    });
  });
  req.end();
}


function remove(client, name, callback) {
  var req = client.del(name);
  logErrors(req);
  console.log('\nFile delete');
  req.on('response', function(res) {
    logResponse(res);
    res.on('data', function(chunk) {
      console.log(chunk);
    });
    res.on('end', function() {
      console.log('Response finished');
      assert.equal(res.statusCode, 204);
      callback();
    });
  });
  req.end();
}


function removeRemoved(client, name, callback) {
  // Trying to removing a nonexistent file, looks the same
  // as removing an existent file (status code 204).
  remove(client, name, callback);
}
