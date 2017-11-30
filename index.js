#!/usr/bin/env node
var pkg = require('./package.json');
var log = require('yalm');
var config = require('./config.js');
log.setLevel(config.verbosity);

log.info(pkg.name + ' ' + pkg.version + ' starting');
log.info('mqtt trying to connect', config.url);
var mqtt = require('mqtt').connect(config.url, { will: { topic: config.name + '/connected', payload: '0' } });
mqtt.publish(config.name + '/connected', '2');

var subscriptions = [
  '+/status/#',
  '+/connected'
];

log.info('connecting InfluxDB', config['influx-host']);

const Influx = require('influx');
const influx = new Influx.InfluxDB({
  host: config['influx-host'],
  port: config['influx-port'],
  database: config['influx-db']
});

var buffer = [];
var bufferCount = 0;

var connected;
mqtt.on('connect', function () {
  connected = true;
  log.info('mqtt connected ' + config.url);

  subscriptions.forEach(function (subs) {
    log.info('mqtt subscribe ' + subs);
    mqtt.subscribe(subs);
  });
});

mqtt.on('close', function () {
  if (connected) {
    connected = false;
    log.info('mqtt closed ' + config.url);
  }
});

mqtt.on('error', function () {
  console.error('mqtt error ' + config.url);
});

mqtt.on('message', function (topic, payload, msg) {
  if (msg.retain) return;
  var timestamp = new Date();

  payload = payload.toString();

  var topicElements = topic.match(/^([^/]+)\/status\/(.+)/);
  if (!topicElements) return;
  var device = topicElements[1];
  var path = topicElements[2];

  var value;
  var valueKey = 'value';
  if (path.includes('Temp') || path.includes('temp')) {
    valueKey = 'temp';
  } else if (path.includes('Strom')) {
    valueKey = 'power';
  }

  try {
    var tmp = JSON.parse(payload);
    value = tmp.val;
    if (tmp.ts) {
      if (tmp.ts < 15120042550) {
        tmp.ts = tmp.ts * 1000;
      }
      timestamp = new Date(tmp.ts);
    }
  } catch (e) {
    value = payload;
  }
  var valueFloat = parseFloat(value);

  if (value === true || value === 1 || value === 'on') {
    value = 1;
  } else if (value === false || value === 0 || value === 'off') {
    value = 0;
  } else if (isNaN(valueFloat)) {
    log.debug('FIXME', value);
    return; // FIXME do we need strings? Creating a field as string leads to errors when trying to write float on it. Can we expect topics to be of the same type always?
    // value = '"' + value + '"';
  } else {
    value = valueFloat;
  }

  log.debug(device, path, value, timestamp, tmp.ts);
  var point = {
    measurement: 'home',
    tags: { device: device, path: path },
    fields: { [valueKey]: value },
    timestamp: timestamp
  };
  buffer.push(point);
  bufferCount += 1;
  if (bufferCount >= 1000) write();
});

function write () {
  if (!bufferCount) return;
  log.debug('write', bufferCount);

  influx.writePoints(buffer).catch(err => {
    console.error(`Error saving data to InfluxDB! ${err.stack}`);
  });

  buffer = [];
  bufferCount = 0;
}

setInterval(write, 60000); // todo command line param
