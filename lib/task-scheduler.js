// Copyright 2015, EMC, Inc.

'use strict';

var di = require('di');

module.exports = taskSchedulerFactory;
di.annotate(taskSchedulerFactory, new di.Provide('TaskGraph.TaskScheduler'));
di.annotate(taskSchedulerFactory,
    new di.Inject(
        'Protocol.Events',
        'TaskGraph.TaskGraph',
        'TaskGraph.Store',
        'Logger',
        'Promise',
        'uuid',
        '_',
        'Rx'
    )
);

function taskSchedulerFactory(
    eventsProtocol,
    TaskGraph,
    store,
    Logger,
    Promise,
    uuid,
    _,
    Rx
) {
    var logger = Logger.initialize(taskSchedulerFactory);
    logger;

    function TaskScheduler(schedulerId) {
        this.schedulerId = schedulerId || uuid.v4();
        this.graphsToEvaluateStream = new Rx.Subject();
        this.pipeline = null;
    }

    TaskScheduler.prototype.initializePipeline = function() {
        var _tasksToScheduleFromGraph =
                this.graphsToEvaluateStream
                    // TODO: refactor to clean up, make a prototype helper function
                    .flatMap(function(graphId) {
                        return store.checkGraphDone(graphId)
                        .then(function(graph) {
                            if (!_.isEmpty(graph)) {
                                return store.findReadyTasksForGraph(graphId);
                            } else {
                                // TODO: use Rx, don't have direct access to AMQP here?
                                eventsProtocol.publishGraphFinished(
                                    graph.instanceId, graph._status);
                                return graph;
                            }
                        });
                    })
                    .flatMap(Rx.Observable.from)
                    .flatMap(store.checkoutTask.bind(this.schedulerId))
                    .filter(function(task) {
                        // Don't schedule items we couldn't check out
                        return !_.isEmpty(task);
                    });

        return [
            _tasksToScheduleFromGraph.subscribe(this.scheduleTaskHandler.bind(this))
        ];
    };

    TaskScheduler.prototype.scheduleTaskHandler = function(task) {
        // TODO: Add more scheduling logic here when necessary
        task;
    };

    TaskScheduler.prototype.handleRunTaskGraphRequest = function(name, options, target) {
        var self = this;

        return Promise.resolve()
        .then(function() {
            var graph = TaskGraph.create(name, options, target);
            return store.persistGraphObject(JSON.stringify(graph));
        })
        .then(function(graph) {
            self.graphsToEvaluateStream.onNext(graph.instanceId);
            self.graphEventStream.onNext(graph.instanceId, 'started');
            return graph.instanceId;
        });
    };

    TaskScheduler.prototype.start = function() {
        var self = this;

        self.pipelines = self.initializePipeline();
    };

    TaskScheduler.prototype.stop = function() {
        _.forEach(this.pipelines, function(pipeline) {
            pipeline.dispose();
        });
    };

    TaskScheduler.create = function() {
        return new TaskScheduler();
    };

    return TaskScheduler;
}
