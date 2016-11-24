'use strict';
var chai = require('chai');
var expect = chai.expect;
var sinon = require('sinon');
var _ = require('lodash');
var hubiquitus = require('hubiquitus-core');
var logger = hubiquitus.logger('hubiquitus:addons:qos:sample');
hubiquitus.logger.enable('hubiquitus:addons:*', 'info');
hubiquitus.logger.enable('hubiquitus:*', 'error');
var hubiquitusQos = require(__dirname + '/../index');
var async = require('async');

describe('HQOS-headers', function () {
  before(function (done) {
    hubiquitusQos.configure(function (err) {
      if (err) {
        return done(err);
      }

      hubiquitus.use(hubiquitusQos.middleware)
        .start({discoveryAddr: 'udp://224.0.0.1:5555'}, done);
    });
  });

  after(function(done) {
    hubiquitus.stop(done);
  });

  it('should propagate headers when sending message safely', function (done) {
    this.timeout(4000);

    var headers = {userInfos: {name: 'hubi'}};

    function sampleActor(req) {
      setTimeout(function () {
        expect(req.headers.userInfos).to.deep.equal(headers.userInfos);
        req.reply();
        done();
      }, 3000);

    }

    hubiquitus.addActor('sampleActor', sampleActor);
    hubiquitusQos.send('god', 'sampleActor', 'do safe work !', headers, function (err, res) {
      process.nextTick(function() {
        expect(err).to.be.undefined;
      });
    });
  });

  it('should propagate headers when PersistAndSend message safely', function (done) {
    this.timeout(4000);

    var headers = {userInfos: {name: 'hubi'}};

    function sampleActor(req) {
      setTimeout(function () {
        expect(req.headers.userInfos).to.deep.equal(headers.userInfos);
        req.reply();
        done();
      }, 3000);
    }

    hubiquitus.addActor('sampleActor2', sampleActor);

    hubiquitusQos.persistAndSend('god/1', 'sampleActor2', 'do safe work !', headers, function (err) {
      process.nextTick(function() {
        expect(err).to.be.undefined;
      });
    });
  });

  it('should propagate headers when answering with error', function (done) {
    var headers = {userInfos: {name: 'hubi'}};

    function sampleActor(req) {
      setTimeout(function() {
        expect(req.headers.userInfos).to.deep.equal(headers.userInfos);
        req.reply({code: 'TEST', msg: 'A sample test error !'});
        done();
      }, 1000)
    }

    hubiquitus.addActor('sampleActor3', sampleActor);

    hubiquitusQos.send('god/1', 'sampleActor3', 'do safe work !', headers, function (err) {
      process.nextTick(function() {
        expect(err).to.be.undefined;
      });
    });
  });

  it('should propagate headers when sending many messages', function(done) {
    this.timeout(4000);
    var nbSend = 1000;

    var headers = [{userInfos: {name: 'hubi'}}, {userInfos: {name: 'hubi2'}}];
    var treated = 0;

    function sampleActor(req) {
      setTimeout(function() {
        expect(req.headers.userInfos).to.deep.equal(headers[req.content % 2].userInfos);
        treated++;
        req.reply();
      }, 100);
    }

    function dosend(num, next) {
      hubiquitusQos.send('god', 'sampleActor4', num, _.cloneDeep(headers[num % 2]), function (err) {
        process.nextTick(function() {
          expect(err).to.be.undefined;
          next();
        });
      });
    }

    function checkresult() {
      if (treated < nbSend) {
        return setTimeout(checkresult, 1000);
      }
      done();
    }

    hubiquitus.addActor('sampleActor4', sampleActor);
    async.times (nbSend, dosend, function (err, res) {
      expect(err).to.be.null;
      checkresult();
    });
  });

  it('should propagate headers when persitAndSend many messages', function(done) {
    this.timeout(4000);
    var nbSend = 1000;

    var headers = [{userInfos: {name: 'hubi'}}, {userInfos: {name: 'hubi2'}}];
    var treated = 0;

    function sampleActor(req) {
      setTimeout(function() {
        expect(req.headers.userInfos).to.deep.equal(headers[req.content % 2].userInfos);
        treated++;
        req.reply();
      }, 100);
    }

    function dosend(num, next) {
      hubiquitusQos.persistAndSend('god', 'sampleActor5', num, _.cloneDeep(headers[num % 2]), function (err) {
        process.nextTick(function() {
          expect(err).to.be.undefined;
          next();
        });
      });
    }

    function checkresult() {
      if (treated < nbSend) {
        return setTimeout(checkresult, 1000);
      }
      done();
    }

    hubiquitus.addActor('sampleActor5', sampleActor);
    async.times (nbSend, dosend, function (err, res) {
      expect(err).to.be.null;
      checkresult();
    });
  });

  it('should propagate headers when resending message with batch', function (done) {
    this.timeout(3000);

    var headers = {userInfos: {name: 'hubi'}};

    function sampleActor(req) {
      process.nextTick(function() {
        expect(req.headers.userInfos).to.deep.equal(headers.userInfos);
        req.reply();
        done();
      });
    }

    hubiquitusQos.send('god/1', 'sampleActor6', 'do safe work !', headers, function (err) {
      process.nextTick(function() {
        expect(err).to.be.undefined;
      });
    });

    setTimeout(function() {
      hubiquitus.addActor('sampleActor6', sampleActor);
      hubiquitusQos.batch.run({
        gcInterval: 15000,
        gcTimeout: 10000
      }, function (err) {
        if (err) process.exit(1);
      })
    }, 2000)
  });

})