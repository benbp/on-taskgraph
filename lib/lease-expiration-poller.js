// Copyright 2015, EMC, Inc.

'use strict';

var di = require('di');

module.exports = leaseExpirationPollerFactory;
di.annotate(leaseExpirationPollerFactory, new di.Provide('TaskGraph.LeaseExpirationPoller'));
di.annotate(leaseExpirationPollerFactory,
    new di.Inject(
        'TaskGraph.Store',
        'Logger',
        'Assert',
        'Rx',
        '_'
    )
);

function leaseExpirationPollerFactory(
    store,
    Logger,
    assert,
    Rx,
    _
) {
    var logger = Logger.initialize(leaseExpirationPollerFactory);

    function LeaseExpirationPoller(scheduler, options) {
        options = options || {};
        assert.object(scheduler);
        assert.string(scheduler.schedulerId);
        this.running = false;
        this.pollInterval = options.pollInterval || 3000;
        this.schedulerId = scheduler.schedulerId;
        this.domain = scheduler.domain;
        this.leaseAdjust = scheduler.pollInterval;
    }

    LeaseExpirationPoller.prototype.pollSchedulerLeases = function() {
        var self = this;

        assert.ok(self.running, 'lease expiration poller is running');

        Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .flatMap(store.findExpiredSchedulerLeases.bind(self, self.domain, self.leaseAdjust))
        .flatMap(function(docs) { return Rx.Observable.from(docs); })
        .flatMap(store.expireSchedulerLease.bind(store))
        .subscribe(function(expired) {
            if (!_.isEmpty(expired)) {
                logger.info('Found expired lease for scheduler', {
                    objectId: expired._id.toString(),
                    expiredSchedulerId: expired.schedulerId,
                    schedulerId: self.schedulerId,
                    domain: self.domain
                });
            }
        }, function(err) {
            logger.error('Error expiring scheduler leases', err);
        });
    };

    LeaseExpirationPoller.prototype.pollTaskRunnerLeases = function() {
        var self = this;

        assert.ok(self.running, 'lease expiration poller is running');

        Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .flatMap(store.findExpiredTaskRunnerLeases.bind(self, self.domain, self.leaseAdjust))
        .flatMap(function(docs) { return Rx.Observable.from(docs); })
        .flatMap(store.expireTaskRunnerLease.bind(store))
        .subscribe(function(expired) {
            if (!_.isEmpty(expired)) {
                logger.info('Found expired lease for TaskRunner', {
                    objectId: expired._id.toString(),
                    expiredTaskRunnerId: expired.taskRunnerId,
                    schedulerId: self.schedulerId,
                    domain: self.domain
                });
            }
        }, function(err) {
            logger.error('Error expiring task runner leases', err);
        });
    };

    LeaseExpirationPoller.prototype.isRunning = function() {
        return this.running;
    };

    LeaseExpirationPoller.prototype.start = function() {
        this.running = true;
        this.pollSchedulerLeases();
        this.pollTaskRunnerLeases();
    };

    LeaseExpirationPoller.prototype.stop = function() {
        this.running = false;
    };

    LeaseExpirationPoller.create = function(scheduler, options) {
        return new LeaseExpirationPoller(scheduler, options);
    };

    return LeaseExpirationPoller;
}
