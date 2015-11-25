// Copyright 2015, EMC, Inc.
/* jshint: node:true */

'use strict';

var di = require('di');

module.exports = Runner;

di.annotate(Runner, new di.Provide('TaskGraph.Runner'));
di.annotate(Runner, new di.Inject(
        'Services.Core',
        //'TaskGraph.Subscriptions',
        'TaskGraph.DataLoader',
        //'TaskGraph.ServiceGraph',
        'TaskGraph.TaskScheduler',
        'TaskGraph.TaskRunner',
        'Task.Messenger'
    )
);

function Runner(core, dataLoader, TaskScheduler, TaskRunner, taskMessenger) {
    function start(options) {
        return core.start()
        .then(function() {
            return dataLoader.start();
        })
        .then(function() {
            return taskMessenger.start();
        })
        .then(function() {
            if (options.runner) { return TaskRunner.create().start(); }
        })
        .then(function() {
            if (options.scheduler) { return TaskScheduler.create().start(); }
        });
        // TODO: re-implement service graph running
    /*
        }).then(function() {
            return taskGraphSubscriptions.start();
        }).then(function() {
            return serviceGraph.start();
    */
    }

    function stop() {
        return core.stop();

        // TODO: add stop for task scheduler and task runners

    /*
        return scheduler.stop().then(function() {
            return serviceGraph.stop();
        }).then(function() {
            return taskGraphSubscriptions.stop();
        }).then(function() {
            return core.stop();
        });
    */
    }

    return {
        start: start,
        stop: stop
    };
}
