// Copyright 2015, EMC, Inc.
//
'use strict';

var di = require('di');

module.exports = taskSchedulerFactory;
di.annotate(taskSchedulerFactory, new di.Provide('TaskGraph.TaskScheduler'));
di.annotate(taskSchedulerFactory,
    new di.Inject(
        'Protocol.Events',
        'Protocol.TaskGraphRunner',
        'Services.Waterline',
        'Logger',
        'Promise',
        'Constants',
        'Assert',
        'uuid',
        '_',
        'Rx'
    )
);

function taskSchedulerFactory(
    eventsProtocol,
    tgrProtocol,
    waterline,
    Logger,
    Promise,
    Constants,
    assert,
    uuid,
    _,
    Rx
) {
    var logger = Logger.initialize(taskSchedulerFactory);

    function TaskScheduler() {
        this.schedulerId = uuid.v4();
        this.subscriptions = [];
        this.streamHandlers = [];
        this.scheduleTaskStream = new Rx.Subject();
        this.graphStateChangeStream = new Rx.Subject();
        this.externalContextStream = new Rx.Subject();
        this.pipeline = null;
    }

    TaskScheduler.prototype.initializePipeline = function() {
        var observables = [
            this.graphStateChangeStream.map(this.evaluateGraphStateHandler.bind(this)),
            this.externalContextStream.flatMap(this.evaluateExternalContextHandler.bind(this))
        ];
        return Rx.Observable.merge(observables).subscribe(this.scheduleTaskHandler.bind(this));
    };

    TaskScheduler.prototype.evaluateExternalContextHandler = function(context) {
        var self = this;
        var promise = waterline.graphobjects.find({
            scheduler: self.schedulerId,
            state: Constants.TaskStates.Pending
        })
        .then(function(graphs) {
            // Not the most efficient to do this nested iteration over
            // everything, but high-traffic changes to context documents
            // aren't really anticipated either. If it turns out this
            // does start happening, then we should re-evaluate whether to
            // create some sort of cache of tasks that are waiting on
            // context document changes.
            return _.flatten(_.map(graphs, function(graph) {
                return _.filter(graph.tasks, function(task) {
                    return self.isTaskReady(graph, task, context);
                });
            }));
        });

        return Rx.Observable.fromPromise(promise);
    };

    TaskScheduler.prototype.evaluateGraphStateHandler = function(graph) {
        var self = this;
        return _.filter(graph.tasks, function(task) {
            if (task.state !== Constants.TaskStates.Pending) {
                return false;
            }
            return self.isTaskReady(task, graph);
        });
    };

    TaskScheduler.prototype.isTaskReady = function(task, graph, context) {
        return _.isEmpty(task.waitingOn) || _.every(task.waitingOn, function(v, k) {
            // Evaluate if the waitOn is a dependency on a context value
            if (k === '$context') {
                return _.has(graph.context, v) || _.has(context, v);
            }
            // Evaluate if the waitOn is a task state
            if (v === 'finished') {
                return _.contains(Constants.FinishedTaskStates, graph.tasks[k].state);
            } else {
                return graph.tasks[k].state === v;
            }
        });
    };

    TaskScheduler.prototype.scheduleTaskHandler = function(task) {
        // TODO: Add more scheduling logic here when necessary
        this.scheduleTaskStream.onNext(task.instanceId);
    };

    TaskScheduler.prototype.scheduleGraph = function(name, options, target) {
        var instanceId = uuid.v4();
        name, options, target;
        return instanceId;
    };

    TaskScheduler.prototype.validateGraph = function(name, options, target) {
        name, options, target;
        return;
    };

    TaskScheduler.prototype.handleRunTaskGraphRequest = function(name, options, target) {
        this.validateGraph(name, options, target);
        var graph = this.createGraph(name, options, target);
        // Trigger this.evaluateGraphStateHandler
        this.graphStateChangeStream.onNext(graph);
        return graph.instanceId;
    };

    TaskScheduler.prototype.start = function() {
        var self = this;
        logger;

        self.pipeline = self.initializePipeline();

        /*
        return tgrProtocol.subscribeRunTaskGraph(self.handleRunTaskGraphRequest.bind(self))
        .then(function(subscription) {
            self.subscriptions.push(subscription);
        })
        // vvv REMOVE, FOR TESTING vvv
        .then(function() {
            return tgrProtocol.runTaskGraph('test', {a: 1}, 'nodeId');
        })
        // ^^^ REMOVE, FOR TESTING ^^^
        .catch(function(err) {
            logger.error('Failed to start TaskScheduler', {
                error: err
            });
        });
        */
    };

    TaskScheduler.prototype.stop = function() {
        if (this.pipeline) {
            this.pipeline.dispose();
        }
    };

    TaskScheduler.create = function() {
        return new TaskScheduler();
    };

    return TaskScheduler;
}
