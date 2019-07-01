//elasticsearch ^16.0.0
const fs = require('fs');
const path = require('path');
const Promise = require('promise');
const retry = require('retry');
/**
  options:{
    interval: 5000,
    waitForActiveShards: true,
    pipeline,
    bufferLimit: 20
  }
**/
const BulkWriter = function BulkWriter(client, options) {
  this.client = client;
  this.options = options;
  this.interval = options.interval || 5000;
  this.waitForActiveShards = options.waitForActiveShards;
  // 流操作
  this.pipeline = options.pipeline;

  this.bulk = []; // 准备bulk的数组
  this.running = false;
  this.timer = false;
  console.log('created', this);
};

BulkWriter.prototype.start = function start() {
  this.checkEsConnection();
  console.log('started');
};

BulkWriter.prototype.stop = function stop() {
  this.running = false;
  if (!this.timer) { return; }
  clearTimeout(this.timer);
  this.timer = null;
  console.log('stopped');
};

BulkWriter.prototype.schedule = function schedule() {
  const thiz = this;
  this.timer = setTimeout(() => {
    thiz.tick();
  }, this.interval);
};

BulkWriter.prototype.tick = function tick() {
  console.log('tick');
  const thiz = this;
  if (!this.running) { return; }
  this.flush()
    .then(() => {
      // finally with last .then()
    })
    .then(() => { // finally()
      thiz.schedule();
    });
};

BulkWriter.prototype.flush = function flush() {
  // write bulk to elasticsearch
  if (this.bulk.length === 0) {
    console.log('nothing to flush');
    return new Promise((resolve) => {
      return resolve();
    });
  }
  const bulk = this.bulk.concat();
  this.bulk = [];
  const body = [];
  bulk.forEach(({ index, type, doc }) => {
    body.push({ index: { _index: index, _type: type, pipeline: this.pipeline } }, doc);
  });
  console.log('bulk writer is going to write', body);
  return this.write(body);
};

BulkWriter.prototype.append = function append(index, type, doc) {
  if (this.options.buffering === true) {
    if (typeof this.options.bufferLimit === 'number' && this.bulk.length >= this.options.bufferLimit) {
      console.log('大于预设长度直接排除');
      return;
    }
    this.bulk.push({
      index, type, doc
    });
  } else {
    this.write([{ index: { _index: index, _type: type, pipeline: this.pipeline } }, doc]);
  }
};

BulkWriter.prototype.write = function write(body) {
  const thiz = this;
  return this.client.bulk({
    body,
    waitForActiveShards: this.waitForActiveShards,
    timeout: this.interval + 'ms',
    type: this.type
  }).then((res) => {
    if (res.errors && res.items) {
      res.items.forEach((item) => {
        if (item.index && item.index.error) {
          console.error('Elasticsearch index error', item.index);
        }
      });
    }
  }).catch((e) => { // 满足条件的重试机制
    const lenSum = thiz.bulk.length + body.length;
    if (thiz.options.bufferLimit && (lenSum >= thiz.options.bufferLimit)) {
      thiz.bulk = body.concat(thiz.bulk.slice(0, thiz.options.bufferLimit - body.length));
    } else {
      thiz.bulk = body.concat(thiz.bulk);
    }
    console.error(e);
    console.log('error occurred', e);
    this.stop();
    this.checkEsConnection();
  });
};
// 链接并执行带有重试机制
BulkWriter.prototype.checkEsConnection = function checkEsConnection() {
  const thiz = this;
  thiz.esConnection = false;
  // 重试机制的预设
  const operation = retry.operation({
    forever: true,
    retries: 1,
    factor: 1,
    minTimeout: 1 * 1000,
    maxTimeout: 60 * 1000,
    randomize: false
  });
  return new Promise((fulfill, reject) => {
    operation.attempt((currentAttempt) => {
      console.log('checking for connection');
      thiz.client.ping().then(
        (res) => {
          thiz.esConnection = true;
          fulfill(true);
          if (thiz.options.buffering === true) {
            console.log('starting bulk writer');
            thiz.running = true;
            thiz.tick();
          }
        },
        (err) => {
          console.log('checking for connection');
          if (operation.retry(err)) {
            return;
          }
          // thiz.esConnection = false;
          reject(new Error('Cannot connect to ES'));
        }
      );
    });
  });
};

module.exports = BulkWriter;
