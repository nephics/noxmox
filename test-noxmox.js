
//
// Copyright(c) 2011 Nephics AB
// MIT Licensed
//

// To run the tests you will need a file called awsauth.json in the parent path.
// The JSON file shall contain an object with the aws key, secret and bucketname.

var fs = require('fs');
var util = require('util');

var nox = require('./nox.js');
var mox = require('./mox.js');

runTests();

function runTests() {
  fs.readFile('../awsauth.json', 'utf8', function(err, data) {
    if (err) {
      console.log(err.message);
      return;
    }
    var options = JSON.parse(data);

    var noxclient = nox.createClient(options);
    console.log('Testing nox client');
    test(noxclient, function(){
      console.log('\nTesting mox client');
      var moxclient = mox.createClient(options);
      test(moxclient, function() {
        console.log('\nAll tests completed');
      });
    });
  });
}


function test(client, callback) {
  var name = 'test/2.txt';
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
    remove(client, name, callback)
  }
}
  

function upload(client, name, buf, callback) {
  console.log('\nFile upload');
  var req = client.put(name, {
    'Content-Type':'text/plain',
    'Content-Length':buf.length
  });
  req.on('error', function(err) {
    console.log(err.message || err);
  });
  req.on('continue', function() {
    req.end(buf);
  });
  req.on('response', function(res) {
    console.log('status code: ' + res.statusCode);
    console.log('headers: ' + util.inspect(res.headers));
    res.on('end', function() {
      console.log('Request finished');
      if (res.statusCode === 200) callback();
    });
  });
}


function stat(client, name, callback) {
  var req = client.head(name);
  req.on('error', function(err) {
    console.log(err.message || err);
  });
  console.log('\nFile stat');
  req.on('response', function(res) {
    console.log('status code: ' + res.statusCode);
    console.log('headers: ' + util.inspect(res.headers));
    res.on('end', function() {
      console.log('Request finished');
      if (res.statusCode === 200) callback();
    });
  });
  req.end();
}

function download(client, name, callback) {
  var req = client.get(name);
  req.on('error', function(err) {
    console.log(err.message || err);
  });
  console.log('\nFile download');
  req.on('response', function(res) {
    console.log('status code: ' + res.statusCode);
    console.log('headers: ' + util.inspect(res.headers));
    var len = 0;
    res.on('data', function(chunk) {
      len += chunk.length;
    });
    res.on('end', function() {
      console.log('Downloaded ' + len + ' bytes of file data');
      if (res.statusCode === 200) callback();
    });
  });
  req.end();
}


function remove(client, name, callback) {
  var req = client.del(name);
  req.on('error', function(err) {
    console.log(err.message || err);
  });
  console.log('\nFile delete');
  req.on('response', function(res) {
    console.log('status code: ' + res.statusCode);
    console.log('headers: ' + util.inspect(res.headers));
    res.on('end', function() {
      console.log('Request finished');
      if (res.statusCode === 204) callback();
    });
  });
  req.end();
}
