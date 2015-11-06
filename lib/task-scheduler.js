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
        'Rx',
        di.Injector
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
    Rx,
    injector
) {
    var logger = Logger.initialize(taskSchedulerFactory);
    logger;

    function TaskScheduler(schedulerId) {
        this.schedulerId = schedulerId || uuid.v4();
        this.evaluateGraphStream = new Rx.Subject();
        this.startGraphStream = new Rx.Subject();
        this.pipeline = null;
    }

    TaskScheduler.prototype.initializePipeline = function() {
        var self = this;
        var readyTaskStream = self.evaluateGraphStream
                .flatMap(store.findReadyTasksForGraph.bind(store));

        var graphDoneSubscription = readyTaskStream
                .filter(function(data) { return _.isEmpty(data.tasks); })
                .map(function(data) { return data.graphId; })
                .subscribe(self.checkGraphDone.bind(self));

        var tasksToSchedule = readyTaskStream
                .filter(function(data) { return !_.isEmpty(data.tasks); })
                .map(function(data) { return data.tasks; })
                .flatMap(Rx.Observable.from)
                .flatMap(store.checkoutTaskForScheduler.bind(store, self.schedulerId))
                .filter(function(task) {
                    // Don't schedule items we couldn't check out
                    return !_.isEmpty(task);
                });

        return [
            tasksToSchedule.subscribe(self.scheduleTaskHandler.bind(self)),
            self.startGraphStream.subscribe(self.handleStartTaskGraphRequest.bind(self)),
            graphDoneSubscription
        ];
    };

    TaskScheduler.prototype.checkGraphDone = function(graphId) {
        // TODO: do this?
        // store.updateUnreachableTasksForGraph(graphId)
        return store.checkGraphDone(graphId)
        .then(function(result) {
            if (result) {
                // TODO: use Rx, abstract the messenger strategy away to another module.
                return eventsProtocol.publishGraphFinished(result.instanceId, result._status);
            }
        })
        .catch(function(error) {
            logger.error('Error checking if graph is finished', {
                graphId: graphId,
                error: error
            });
        });
    };

    TaskScheduler.prototype.scheduleTaskHandler = function(task) {
        // TODO: Add more scheduling logic here when necessary
        logger.debug('Schedule task handler called ' + task.instanceId);
    };

    // TODO: refactor this into its own observable pipeline, no reason
    // to stick a big promise chain in here when everything else is simplified
    // into smaller observable callback chunks.
    TaskScheduler.prototype.handleStartTaskGraphRequest = function(name, options, target) {
        var self = this;

        return Promise.resolve()
        .then(function() {
            var definition = {
                friendlyName: 'noop-graph',
                injectableName: 'Graph.noop-example',
                tasks: [
                    {
                        label: 'noop-1',
                        taskName: 'Task.noop'
                    },
                    {
                        label: 'noop-2',
                        taskName: 'Task.noop',
                        waitOn: {
                            'noop-1': 'finished'
                        }
                    },
                    {
                        label: 'parallel-noop-1',
                        taskName: 'Task.noop',
                        waitOn: {
                            'noop-2': 'finished'
                        }
                    },
                    {
                        label: 'parallel-noop-2',
                        taskName: 'Task.noop',
                        waitOn: {
                            'noop-2': 'finished'
                        }
                    },
                ]
            };
            //var definition = injector.get(name);
            return TaskGraph.create(definition, options, target);
        })
        .then(function(graph) {
            var _graph = graph.toJSON();
            return [
                store.persistGraphObject(_graph),
                Promise.map(_.values(_graph.tasks), function(task) {
                    return Promise.map(self.createTaskDependencyItems(task), function(_task) {
                        return store.persistTaskDependencies(_task, _graph.instanceId);
                    });
                })
            ];
        })
        .spread(function(result) {
            self.evaluateGraphStream.onNext(result.instanceId);
        })
        .catch(function(error) {
            logger.error('Failure handling start taskgraph request', {
                error: error,
                name: name,
                target: target,
                options: options
            });
        });
    };

    TaskScheduler.prototype.createTaskDependencyItems = function(task) {
        return [task];
    };

    TaskScheduler.prototype.start = function() {
        this.pipelines = this.initializePipeline();

        // test
        this.test();
    };

    TaskScheduler.prototype.test = function() {
        this.startGraphStream.onNext('test');
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
