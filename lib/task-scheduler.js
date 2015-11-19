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
        'Task.Messenger'
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
    taskMessenger
) {
    var logger = Logger.initialize(taskSchedulerFactory);

    function TaskScheduler(options) {
        options = options || {};
        this.running = false;
        this.schedulerId = options.schedulerId || uuid.v4();
        this.domain = options.domain || 'default';
        this.evaluateTaskStream = new Rx.Subject();
        this.evaluateGraphStream = new Rx.Subject();
        this.startGraphStream = new Rx.Subject();
        this.pipeline = null;
    }

    TaskScheduler.prototype.initializePipeline = function() {
        var taskHandlerStream = this.createTaskHandlerStream(this.evaluateTaskStream);
        var readyTaskStream = this.createReadyTaskStream(this.evaluateGraphStream);

        return [
            this.createGraphFailSubscription(taskHandlerStream),
            this.createUpdateTaskDependenciesSubscription(taskHandlerStream),
            this.createTasksToScheduleSubscription(readyTaskStream),
            this.createStartTaskGraphSubscription(this.startGraphStream),
            this.createGraphDoneSubscription(readyTaskStream)
        ];
    };

    TaskScheduler.prototype.isRunning = function() {
        return this.running;
    };

    TaskScheduler.prototype.createTaskHandlerStream = function(evaluateTaskStream) {
        return evaluateTaskStream
                .takeWhile(this.isRunning.bind(this))
                .flatMap(this.checkTaskStateHandled.bind(this))
                .catch(this.handleStreamError.bind(this, 'Error evaluating task state'))
                .share();
    };

    TaskScheduler.prototype.createReadyTaskStream = function(evaluateGraphStream) {
        return evaluateGraphStream
                .takeWhile(this.isRunning.bind(this))
                .flatMap(store.findReadyTasksForGraph.bind(store))
                .catch(this.handleStreamError.bind(this, 'Error finding ready tasks'))
                .share();
    };

    TaskScheduler.prototype.createGraphFailSubscription = function(taskHandlerStream) {
        var self = this;
        return taskHandlerStream
                .takeWhile(self.isRunning.bind(self))
                .filter(function(data) { return data.unhandledFailure; })
                .flatMap(self.failGraph.bind(self))
                .subscribe(
                    self.handleStreamSuccess.bind(
                        self, 'Graph failed due to unhandled task failure'),
                    self.handleStreamError.bind(self, 'Error failing graph')
                );
    };

    TaskScheduler.prototype.createUpdateTaskDependenciesSubscription = function(taskHandlerStream) {
        var self = this;
        return taskHandlerStream
                .takeWhile(self.isRunning.bind(self))
                .filter(function(data) { return !data.unhandledFailure; })
                .flatMap(function(data) {
                    return Rx.Observable.forkJoin([
                        Rx.Observable.just(data),
                        store.updateDependentTasks(data),
                        store.updateUnreachableTasks(data)
                    ]);
                })
                .map(_.first)
                .flatMap(store.markTaskEvaluated.bind(store))
                .subscribe(
                    self.evaluateGraphStream.onNext.bind(self.evaluateGraphStream),
                    self.handleStreamError.bind(self, 'Error updating task dependencies')
                );
    };

    TaskScheduler.prototype.createTasksToScheduleSubscription = function(readyTaskStream) {
        var self = this;
        return readyTaskStream
                .takeWhile(self.isRunning.bind(self))
                .filter(function(data) { return !_.isEmpty(data.tasks); })
                .map(function(data) { return data.tasks; })
                .flatMap(function(tasks) { return Rx.Observable.from(tasks); })
                .flatMap(store.checkoutTaskForScheduler.bind(store, self.schedulerId))
                // Don't schedule items we couldn't check out
                .filter(function(task) { return !_.isEmpty(task); })
                .flatMap(self.scheduleTaskHandler.bind(self))
                .subscribe(
                    self.handleStreamSuccess.bind(self, 'Task scheduled'),
                    self.handleStreamError.bind(self, 'Error scheduling task')
                );
    };

    TaskScheduler.prototype.createStartTaskGraphSubscription = function(startGraphStream) {
        var self = this;
        return startGraphStream
                    .takeWhile(this.isRunning.bind(this))
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
                .takeWhile(self.isRunning.bind(self))
                .filter(function(data) { return _.isEmpty(data.tasks); })
                .flatMap(store.checkGraphDone.bind(store))
                .filter(function(data) { return data.done; })
                .flatMap(self.publishGraphDone.bind(self))
                .subscribe(
                    self.handleStreamSuccess.bind(self, null),
                    self.handleStreamError.bind(self, 'Error checking graph done')
                );
    };

    TaskScheduler.prototype.handleStreamSuccess = function(msg, data) {
        if (msg) {
            if (data && !data.schedulerId) {
                data.schedulerId = this.schedulerId;
            }
            logger.debug(msg, data);
        }
        return Rx.Observable.empty();
    };

    TaskScheduler.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            schedulerId: this.schedulerId,
            error: err
        });
        return Rx.Observable.empty();
    };

    TaskScheduler.prototype.subscribeTaskFinished = function() {
        return taskMessenger.subscribeTaskFinished(
                this.domain,
                this.evaluateTaskStream.onNext.bind(this.evaluateTaskStream)
            );
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

    TaskScheduler.prototype.scheduleTaskHandler = function(data) {
        // TODO: Add more scheduling logic here when necessary
        return taskMessenger.publishRunTask('default', data.instanceId, data.graphId)
        // TODO: make retryable, or coerce to Rx observable so we can just
        // use that logic
        .catch(function(err) {
            logger.error("Error publishing run task event", {
                error: err
            });
        });
    };

    TaskScheduler.prototype.persistInitialGraphAndTaskState = function(graph) {
        // TODO: can this be refactored with forkJoin?
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

    TaskScheduler.prototype.failGraph = function(data) {
        return Promise.resolve(data);
    };

    TaskScheduler.prototype.start = function() {
        var self = this;
        return Promise.resolve()
        .then(function() {
            self.running = true;
            self.pipelines = self.initializePipeline();
            return self.subscribeTaskFinished();
        })
        // TODO: remove test tap
        .tap(function() {
            self.test();
        });
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
        try {
            this.running = false;
            while (!_.isEmpty(this.pipelines)) {
                this.pipelines.pop().dispose();
            }
        } catch (e) {
            logger.error('Failed to stop task scheduler', {
                schedulerId: this.schedulerId,
                error: e
            });
        }
    };

    TaskScheduler.create = function() {
        return new TaskScheduler();
    };

    return TaskScheduler;
}
