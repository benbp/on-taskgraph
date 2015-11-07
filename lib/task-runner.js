// Copyright 2015, EMC, Inc.
//
'use strict';

var di = require('di');
module.exports = taskRunnerFactory;
di.annotate(taskRunnerFactory, new di.Provide('TaskGraph.TaskRunner'));
di.annotate(taskRunnerFactory,
    new di.Inject(
        'Logger',
        'Promise',
        'Constants',
        'Assert',
        'uuid',
        '_',
        'Rx',
        'Task.Task',
        'Task.Messenger',
        'TaskGraph.Store'
    )
);

function taskRunnerFactory(
    Logger,
    Promise,
    Constants,
    assert,
    uuid,
    _,
    Rx,
    Task,
    taskMessenger,
    store
) {
    var logger = Logger.initialize(taskRunnerFactory);

    function TaskRunner( domain) {
        this.taskRunnerId = uuid.v4();
        this.inputStream = new Rx.Subject();
        this.subscriptions = [];
        this.pipeline = null;
        this.heart = null;
        this.activeTasks = {};
        this.domain = domain || 'defaultDomain';
    }

    TaskRunner.prototype.start = function() {
        this.initializePipeline();
        this.startHeart();
        return taskMessenger.subscribeRun(this.domain, this.inputStream.onNext);
    };

    TaskRunner.prototype.initializePipeline = function() {
        var self = this;
        this.pipeline = this.inputStream.flatMap(function(taskAndGraphId) {
            return store.checkoutTaskForRunner(self.taskRunnerId, taskAndGraphId);
        }, function(taskAndGraphId, maybeNullTask) {
            return {
                definition: maybeNullTask,
                graphId: taskAndGraphId.graphId
            };
        })
        .filter(function(task) {
            return _.isEmpty(task.definition) ? false : true;
        });

        this.subscriptions.push(
                this.pipeline.subscribe(
                    this.handleTask
                )
            );
    };

    TaskRunner.prototype.startHeart = function(interval) {
        interval = interval || 2000;
        this.heart = setInterval(store.heartbeatTasks.bind(store, this.taskRunnerId), interval);
    };

    TaskRunner.prototype.stop = function() {
        clearInterval(this.heart);
        return this.inputStream.dispose();
    };

    function getContext() {return {};}

    TaskRunner.prototype.handleTask = function(task) {
        var self = this;
        console.log(task);
        var taskInstance = Task.create(
                task.definition,
                {}/*Overrides?*/,
                getContext()/*or something?*/
            );
        logger.debug("Running task ", {data: taskInstance});
        this.activeTasks[taskInstance.taskId] = taskInstance;
        return taskInstance.run()
        .finally(function() {
            delete self.activeTasks[taskInstance.taskId];
        });
    };

    return TaskRunner;
}
