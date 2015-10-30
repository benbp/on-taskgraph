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
        this.pipeline = this.inputStream.flatMap(function(stuff) {
            return waterline.graphobjects.checkoutTaskForRunner(stuff);
        })
        .filter(function(task) {
            return _.isEmpty(task) ? false : true;
        })
        .map(function(myTask) {
            return {
                definition: myTask,
                graphId: 'bleh'
            };
        });

        this.subscriptions.push(this.pipeline.subscribe(
                    this.handleTask,
                    this.handleError,
                    this.handleComplete
        ));
    };

    TaskRunner.prototype.stop = function() {
        return this.inputStream.dispose();
    };

    function getContext() {return {};}

    TaskRunner.prototype.handleTask = function(task) {
        console.log(task);
        var taskInstance = Task.create(task.definition,{}/*Overrides?*/, getContext()/*or something?*/);
    /*
        taskInstance.instantiateJob(); // ????? instantiate?
        taskInstance.
        //run the task
        //report done
    */
        taskInstance.run(); // !??!?!?!!??! does it all?
    };

    TaskRunner.prototype.handleError = function(err) {
        //handle the error?
    };

    TaskRunner.prototype.handleComplete = function() {
        //do something?
    };

    return TaskRunner;
}
