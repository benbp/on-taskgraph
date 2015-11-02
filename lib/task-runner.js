// Copyright 2015, EMC, Inc.
//
'use strict';

var di = require('di');
module.exports = taskRunnerFactory;
di.annotate(taskRunnerFactory, new di.Provide('TaskGraph.TaskRunner'));
di.annotate(taskRunnerFactory,
    new di.Inject(
        'Protocol.Events',
        'Logger',
        'Promise',
        'Constants',
        'Assert',
        'uuid',
        '_',
        'Rx',
        'Task.Task',
        'TaskGraph.Store'
    )
);

function taskRunnerFactory(
    eventsProtocol,
    Logger,
    Promise,
    Constants,
    assert,
    uuid,
    _,
    Rx,
    Task,
    store
) {
    var logger = Logger.initialize(taskRunnerFactory);

    function TaskRunner() {
        this.taskRunnerId = uuid.v4();
        this.inputStream = new Rx.Subject();
        this.subscriptions = [];
        this.pipeline = null;
    }

    TaskRunner.prototype.start = function() {

        this.pipeline = this.inputStream.flatMap(function(taskAndGraphId) {
            return store.checkoutTask(taskAndGraphId.taskId);
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

    TaskRunner.prototype.stop = function() {
        return this.inputStream.dispose();
    };

    function getContext() {return {};}

    TaskRunner.prototype.handleTask = function(task) {
        console.log(task);
        var taskInstance = Task.create(task.definition,{}/*Overrides?*/, getContext()/*or something?*/);
        taskInstance.run();
    };

    return TaskRunner;
}
