// Copyright 2015, EMC, Inc.
//
'use strict';

var di = require('di');
module.exports = taskRunnerFactory;
di.annotate(taskRunnerFactory, new di.Provide('TaskGraph.TaskRunner'));
di.annotate(taskRunnerFactory,
    new di.Inject(
        'Protocol.Events',
        'Services.Waterline',
        'Logger',
        'Promise',
        'Constants',
        'Assert',
        'uuid',
        '_',
        'Rx',
        'Task.Task'
    )
);

function taskRunnerFactory(
    eventsProtocol,
    waterline,
    Logger,
    Promise,
    Constants,
    assert,
    uuid,
    _,
    Rx,
    Task
) {
    var logger = Logger.initialize(taskRunnerFactory);

    function TaskRunner() {
        this.taskRunnerId = uuid.v4();
        this.inputStream = new Rx.Subject();
        this.subscriptions = [];
        this.pipeline = null;
    }

    TaskRunner.prototype.start = function() {
        var self = this;
        this.pipeline = this.inputStream.flatMap(waterline.graphobjects.checkoutTaskForRunner)
        .filter(function(task) {
            return _.isEmpty(task) ? true : false;
        });
    };

    TaskRunner.prototype.stop = function() {
        return this.inputStream.dispose();
    };


    TaskRunner.prototype.handleTask = function(task) {
        //instantiate
        //run the task
        //report done
    };

    TaskRunner.prototype.handleError = function(err) {
        //handle the error
    };

    TaskRunner.prototype.handleComplete = function() {
        //do something?
    };

    return TaskRunner;
}
