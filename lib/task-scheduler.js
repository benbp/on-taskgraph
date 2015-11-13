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
        'Constants',
        'Logger',
        'Promise',
        'uuid',
        '_',
        'Rx',
        'Task.Messenger',
        di.Injector
    )
);

function taskSchedulerFactory(
    eventsProtocol,
    TaskGraph,
    store,
    Constants,
    Logger,
    Promise,
    uuid,
    _,
    Rx,
    taskMessenger,
    injector
) {
    var logger = Logger.initialize(taskSchedulerFactory);

    function TaskScheduler(schedulerId) {
        this.schedulerId = schedulerId || uuid.v4();
        this.evaluateTaskStream = new Rx.Subject();
        this.evaluateGraphStream = new Rx.Subject();
        this.startGraphStream = new Rx.Subject();
        this.pipeline = null;
    }

    TaskScheduler.prototype.initializePipeline = function() {
        var self = this;

        var taskHandlerStream = self.evaluateTaskStream
                .flatMap(self.checkTaskStateHandled.bind(self))
                .share();

        var readyTaskStream = self.evaluateGraphStream
                .flatMap(store.findReadyTasksForGraph.bind(store))
                // TODO: does calling .share() mean errors are multicast to
                // each subscriber error handler? If so, how to prevent that
                // if errors occur before the multicast?
                .share();

        return [
            self.createGraphFailSubscription(taskHandlerStream),
            self.createUpdateTaskDependenciesSubscription(taskHandlerStream),
            self.createTasksToScheduleSubscription(readyTaskStream),
            self.createStartTaskGraphSubscription(self.startGraphStream),
            self.createGraphDoneSubscription(readyTaskStream)
        ];
    };

    TaskScheduler.prototype.createGraphFailSubscription = function(taskHandlerStream) {
        var self = this;
        return taskHandlerStream
                .filter(function(data) { return data.unhandledFailure; })
                .subscribe(
                    self.failGraph.bind(self),
                    self.handleStreamError.bind(self, 'An error occurred handling task finished')
                );
    };

    TaskScheduler.prototype.createUpdateTaskDependenciesSubscription = function(taskHandlerStream) {
        var self = this;
        return taskHandlerStream
                .filter(function(data) { return !data.unhandledFailure; })
                .flatMap(store.updateDependentTasks.bind(store))
                .flatMap(store.updateUnreachableTasks.bind(store))
                .flatMap(store.markTaskEvaluated.bind(store))
                .subscribe(
                    self.evaluateGraphStream.onNext.bind(self.evaluateGraphStream),
                    self.handleStreamError.bind(self, 'Error updating task dependencies')
                );
    };

    TaskScheduler.prototype.createTasksToScheduleSubscription = function(readyTaskStream) {
        var self = this;
        return readyTaskStream
                .filter(function(data) { return !_.isEmpty(data.tasks); })
                .map(function(data) { return data.tasks; })
                .flatMap(Rx.Observable.from)
                .flatMap(store.checkoutTaskForScheduler.bind(store, self.schedulerId))
                // Don't schedule items we couldn't check out
                .filter(function(task) { return !_.isEmpty(task); })
                .subscribe(
                    self.scheduleTaskHandler.bind(self),
                    self.handleStreamError.bind(self, 'Error scheduling task')
                );
    };

    TaskScheduler.prototype.createStartTaskGraphSubscription = function(startGraphStream) {
        var self = this;
        return startGraphStream
                    .flatMap(TaskGraph.create)
                    .flatMap(self.persistInitialGraphAndTaskState.bind(self))
                    .subscribe(
                        self.evaluateGraphStream.onNext.bind(self.evaluateGraphStream),
                        self.handleStreamError.bind(self, 'Error starting task graph')
                    );
    };

    TaskScheduler.prototype.createGraphDoneSubscription = function(readyTaskStream) {
        var self = this;
        return readyTaskStream
                .filter(function(data) { return _.isEmpty(data.tasks); })
                .map(function(data) { return data.graphId; })
                .flatMap(store.checkGraphDone.bind(store))
                .filter(Boolean)
                .subscribe(
                    self.publishGraphDone.bind(self),
                    self.handleStreamError.bind(self, 'Error checking graph done')
                );
    };


    TaskScheduler.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            error: err
        });
    };

    TaskScheduler.prototype.publishGraphDone = function(graphId) {
        return store.setGraphDone(graphId)
        .then(function(graph) {
            // TODO: mark graph state as done here in the db?
            // TODO: use Rx, abstract the messenger strategy away to another module.
            return eventsProtocol.publishGraphFinished(graphId, graph._status);
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
        return taskMessenger.publishRunTask('default', task)
        .catch(function(err) {
            logger.error("Error publishing task", {error:err});
        });
    };

    TaskScheduler.prototype.persistInitialGraphAndTaskState = function(graph) {
        return Promise.all([
            store.persistGraphObject(graph),
            Promise.map(graph.createTaskDependencyItems(), function(item) {
                return store.persistTaskDependencies(item, graph.instanceId);
            })
        ])
        .then(function() {
            return {
                graphId: graph.instanceId
            };
        });
    };

    TaskScheduler.prototype.checkTaskStateHandled = function(data) {
        if (_.contains(Constants.TaskStates.FailedTaskStates, data.taskState)) {
            return store.isTaskFailureHandled(data.graphId, data.taskId, data.taskState)
            .then(function(handled) {
                data.unhandledFailure = !handled;
                return data;
            });
        } else {
            data.unhandledFailure = false;
            return Promise.resolve(data);
        }
    };

    TaskScheduler.prototype.handleTaskFinished = function(data) {
        if (data.unhandledFailure) {
            return this.failGraph(data)
            .then(function() {
                return data;
            });
        } else {
            return Promise.resolve(data);
        }
    };

    TaskScheduler.prototype.failGraph = function(data) {
        data;
    };

    TaskScheduler.prototype.start = function() {
        this.pipelines = this.initializePipeline();

        // TODO: remove test
        this.test();
    };

    TaskScheduler.prototype.test = function() {
        var testObj = { definition: {
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
                        'noop-1': 'finished',
                        'noop-2': ['finished', 'timeout']
                    }
                },
                {
                    label: 'parallel-noop-2',
                    taskName: 'Task.noop',
                    waitOn: {
                        'noop-1': ['finished'],
                        'noop-2': ['finished', 'timeout']
                    }
                }
            ]
        }};
        this.startGraphStream.onNext(testObj);
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
