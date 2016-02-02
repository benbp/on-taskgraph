// Copyright 2016, EMC, Inc.

'use strict';

var di = require('di');

module.exports = taskSchedulerFactory;
di.annotate(taskSchedulerFactory, new di.Provide('TaskGraph.TaskScheduler'));
di.annotate(taskSchedulerFactory,
    new di.Inject(
        'Protocol.Events',
        'TaskGraph.TaskGraph',
        'TaskGraph.Store',
        'TaskGraph.LeaseExpirationPoller',
        'Constants',
        'Logger',
        'Promise',
        'uuid',
        'Assert',
        '_',
        'Rx.Mixins',
        'Task.Messenger'
    )
);

function taskSchedulerFactory(
    eventsProtocol,
    TaskGraph,
    store,
    LeaseExpirationPoller,
    Constants,
    Logger,
    Promise,
    uuid,
    assert,
    _,
    Rx,
    taskMessenger
) {
    var logger = Logger.initialize(taskSchedulerFactory);

    /**
     * The TaskScheduler handles all graph and task evaluation, and is
     * the decision engine for scheduling new tasks to be run within a graph.
     *
     * @param {Object} options
     * @constructor
     */
    function TaskScheduler(options) {
        options = options || {};
        this.running = false;
        this.schedulerId = options.schedulerId || uuid.v4();
        this.domain = options.domain || Constants.DefaultTaskDomain;
        this.evaluateTaskStream = new Rx.Subject();
        this.evaluateGraphStream = new Rx.Subject();
        this.checkGraphFinishedStream = new Rx.Subject();
        this.pollInterval = options.pollInterval || 500;
        this.concurrencyMaximums = this.getConcurrencyMaximums(options.concurrent);
        this.findUnevaluatedTasksLimit = 200;
        this.subscriptions = [];
        this.leasePoller = null;
        this.debug = false;
    }

    /**
     * Generate a concurrency counter object. This is used with the
     * Rx.Observable.prototype.mergeLossy function from Rx.Mixins to keep
     * ensure that a specified maximum of the same type of asynchronous
     * call be able to be unresolved at the same time (for example, only
     * wait on a max of 100 database calls to resolve at any point in time).
     *
     * @param {Number} max
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.concurrentCounter = function(max) {
        assert.number(max);
        return {
            count: 0,
            max: max
        };
    };

    /**
     * Generate concurrency counter objects for the different IO calls we make
     * to the store and messenger. Basically a rudimentary method of adding throttling.
     *
     * @param {Object} concurrentOptions
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.getConcurrencyMaximums = function(concurrentOptions) {
        var self = this;
        var _options = _.defaults(concurrentOptions || {}, {
            // Favor evaluateGraphStream, since it's what results in actual
            // scheduling. If we're at load, defer evaluation in favor of scheduling.
            // TODO: Probably better as a priority queue, evaluateGraphStream events
            // always come up first?
            findReadyTasks: 100,
            updateTaskDependencies: 100,
            handleScheduleTaskEvent: 100,
            completeGraphs: 100,
            findUnevaluatedTasks: 1
        });
        return _.transform(_options, function(result, v, k) {
            result[k] = self.concurrentCounter(v);
        }, {});
    };

    /**
     * Create and start the Rx observables (streams) that do all the scheduling work.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.initializePipeline = function() {
        assert.ok(this.running, 'scheduler is running');

        // Inputs from this.evaluateTaskStream
        // Outputs to this.evaluateGraphStream
        // Outputs to this.checkGraphFinishedStream
        this.createUpdateTaskDependenciesSubscription(
                this.evaluateTaskStream, this.evaluateGraphStream, this.checkGraphFinishedStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Task evaluated'),
            this.handleStreamError.bind(this, 'Error at update task dependencies stream')
        );
        // Outputs to this.evaluateTaskStream
        this.createUnevaluatedTaskPollerSubscription(this.evaluateTaskStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Triggered evaluate task event'),
            this.handleStreamError.bind(this, 'Error polling for tasks')
        );
        // Outputs to this.evaluateGraphStream
        this.createEvaluatedTaskPollerSubscription(this.evaluateGraphStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Triggered evaluate graph event'),
            this.handleStreamError.bind(this, 'Error polling for tasks')
        );
        // Inputs from this.evaluateGraphStream
        this.createTasksToScheduleSubscription(this.evaluateGraphStream)
        .subscribe(
            this.handleStreamSuccess.bind(this, 'Task scheduled'),
            this.handleStreamError.bind(this, 'Error at task scheduling stream')
        );
        // Inputs from this.checkGraphFinishedStream
        this.createCheckGraphFinishedSubscription(this.checkGraphFinishedStream)
        .subscribe(
            this.handleStreamSuccess.bind(this, 'Graph finished'),
            this.handleStreamError.bind(this, 'Error at check graph finished stream')
        );
    };

    /**
     * This is used with Rx.Observable.prototype.takeWhile in each Observable
     * created by TaskScheduler.prototype.initializePipeline. When isRunning()
     * returns false, all the observables will automatically dispose.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.isRunning = function() {
        return this.running;
    };

    /**
     * This finds all tasks that are ready to run. If a graphId is
     * specified in the data object, then it will only find tasks ready
     * to run that are within that graph.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.findReadyTasks = function(data) {
        assert.object(data);
        var self = this;

        return Rx.Observable.just(data)
        .flatMap(function() {
            return store.findReadyTasks(self.domain, data.graphId);
        })
        .catch(self.handleStreamError.bind(self, 'Error finding ready tasks'));
    };

    /**
     * This handles task finished events, and updates all other tasks that
     * have a waitingOn dependency on the finished task.
     *
     * @param {Object} taskHandlerStream
     * @param {Object} evaluateGraphStream
     * @param {Object} checkGraphFinishedStream
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createUpdateTaskDependenciesSubscription =
        function(taskHandlerStream, evaluateGraphStream, checkGraphFinishedStream) {

        var self = this;

        return taskHandlerStream
        .takeWhile(self.isRunning.bind(self))
        .tap(self.handleStreamDebug.bind(self, 'Received evaluate task event'))
        .map(self.updateTaskDependencies.bind(self))
        .mergeLossy(self.concurrencyMaximums.updateTaskDependencies)
        .tap(function(task) {
            var _task = _.pick(task, ['domain', 'graphId', 'taskId']);
            self.handleStreamDebug('Updated dependencies for task', _task);
        })
        .filter(function(data) { return data; })
        .map(self.handleEvaluatedTask.bind(self, checkGraphFinishedStream, evaluateGraphStream));
    };

    /**
     * Once a task has finished and been evaluated (dependendent tasks updated)
     * then check if the task is terminal to determine whether the graph is potentially
     * completed or not. If it is not terminal, emit to evaluateGraphStream which
     * will trigger findReadyTasks for that graph. If it is terminal, check if
     * the graph is finished.
     *
     * @param {Object} checkGraphFinishedStream
     * @param {Object} evaluateGraphStream
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleEvaluatedTask = function(
            checkGraphFinishedStream, evaluateGraphStream, data) {
        if (_.contains(data.terminalOnStates, data.state)) {
            checkGraphFinishedStream.onNext(data);
        } else {
            evaluateGraphStream.onNext({ graphId: data.graphId });
        }
    };

    /**
     * Stream handler that finds tasks that are ready to schedule and schedules
     * them.
     *
     * @param {Object} evaluateGraphStream
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createTasksToScheduleSubscription = function(evaluateGraphStream) {
        var self = this;
        return evaluateGraphStream
        .takeWhile(self.isRunning.bind(self))
        .map(self.findReadyTasks.bind(self))
        .mergeLossy(self.concurrencyMaximums.findReadyTasks)
        .filter(function(data) { return !_.isEmpty(data.tasks); })
        .pluck('tasks')
        .flatMap(function(tasks) { return Rx.Observable.from(tasks); })
        .map(self.handleScheduleTaskEvent.bind(self))
        .mergeLossy(self.concurrencyMaximums.handleScheduleTaskEvent)
        .map(function(task) {
            return _.pick(task, ['domain', 'graphId', 'taskId']);
        });
    };

    /**
     * Publish a task schedule event with the messenger.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleScheduleTaskEvent = function(data) {
        var self = this;
        assert.object(data, 'task data object');

        return Rx.Observable.just(data)
        .flatMap(self.publishScheduleTaskEvent.bind(self))
        .catch(self.handleStreamError.bind(self, 'Error scheduling task'));
    };

    /**
     * Determine whether a graph is finished or failed based on a terminal
     * task state.
     *
     * @param {Object} checkGraphFinishedStream
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createCheckGraphFinishedSubscription = function(
            checkGraphFinishedStream) {
        var self = this;

        return checkGraphFinishedStream
        .takeWhile(self.isRunning.bind(self))
        .map(function(data) {
            // We already know that the task in question is in a terminal state,
            // otherwise we wouldn't have published data to this stream.
            // If a task is in a failed task state but it is non-terminal, this
            // code will not be reached.
            if (_.contains(Constants.FailedTaskStates, data.state)) {
                return self.failGraph(data, Constants.TaskStates.Failed);
            } else {
                return self.checkGraphSucceeded(data);
            }
        })
        .mergeLossy(self.concurrencyMaximums.completeGraphs);
    };

    /**
     * Check if a graph is finished. If so, mark it as successful
     * in the store.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.checkGraphSucceeded = function(data) {
        assert.object(data, 'graph data object');
        var self = this;

        return Rx.Observable.just(data)
        .flatMap(store.checkGraphFinished.bind(store))
        .filter(function(_data) { return _data.done; })
        .flatMap(store.setGraphDone.bind(store, Constants.TaskStates.Succeeded))
        .filter(function(graph) { return !_.isEmpty(graph); })
        .map(function(graph) { return _.pick(graph, ['instanceId', '_status']); })
        .tap(self.publishGraphFinished.bind(self))
        .catch(self.handleStreamError.bind(self, 'Error handling graph done event'));
    };

    /**
     * Set a graph to a failed state in the store, and find pending tasks
     * within the graph that should also be failed.
     *
     * @param {Object} data
     * @param {String} graphState
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.failGraph = function(data, graphState) {
        return Rx.Observable.just(data.graphId)
        .flatMap(store.getActiveGraphById)
        .filter(function(graph) {return !_.isEmpty(graph);})
        .map(function(doneGraph) {
            return _.map(doneGraph.tasks, function(taskObj) {
                if(taskObj.state ===  Constants.TaskStates.Pending) {
                    taskObj.state = graphState;
                }
                taskObj.taskId = taskObj.instanceId;
                taskObj.graphId = data.graphId;
                return taskObj;
            });
        })
        .flatMap(this.handleFailGraphTasks.bind(this))
        .flatMap(store.setGraphDone.bind(store, graphState, data))
        // setGraphDone can return null if another source has already updated
        // the graph state. Don't publish the same event twice.
        .filter(function(graph) { return graph; })
        .tap(this.publishGraphFinished.bind(this))
        .catch(this.handleStreamError.bind(this, 'Error failing/cancelling graph'));
    };

    /**
     * Fail pending tasks within a graph.
     *
     * @param {Array} tasks
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleFailGraphTasks = function(tasks) {
        logger.debug('cancel/failing pending tasks', {data:_.pluck(tasks, 'taskId')});
        return Rx.Observable.just(tasks)
        .flatMap(Promise.map.bind(Promise, tasks, store.setTaskState))
        .flatMap(Promise.map.bind(Promise, tasks, store.markTaskEvaluated))
        .flatMap(Promise.map.bind(Promise, _.pluck(tasks, 'taskId'),
                    taskMessenger.publishCancelTask))
        .flatMap(Promise.map.bind(Promise, tasks, store.setTaskStateInGraph.bind(store)));
    };

    /**
     * Subscribe to messenger events to cancel a graph, and fail the graph on
     * received events.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.subscribeCancelGraph = function() {
        var self = this;
        return taskMessenger.subscribeCancelGraph(
            function(data) {
                logger.debug('listener received cancel graph event', {
                    data: data,
                    schedulerId: self.schedulerId
                });
                self.failGraph(data, Constants.TaskStates.Cancelled)
                .map(function() { return data; })
                .subscribe(self.handleStreamSuccess.bind(self, 'Graph Cancelled'));
            }
        );
    };

    /**
     * Evaluate and update all tasks that have a waitingOn dependency for a finished task,
     * then mark the finished task as evaluated. If a failure occurs before the
     * store.markTaskEvaluated call, then this process (which is idempotent) will be
     * repeated on the next poll interval. This basically enables some sort of
     * an equivalent to a transactional database call in failure cases.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.updateTaskDependencies = function(data) {
        assert.object(data, 'task dependency object');
        return Rx.Observable.forkJoin([
            store.setTaskStateInGraph(data),
            store.updateDependentTasks(data),
            store.updateUnreachableTasks(data)
        ])
        .flatMap(store.markTaskEvaluated.bind(store, data))
        .catch(this.handleStreamError.bind(this, 'Error updating task dependencies'));
    };

    /**
     * Log handler for observable onNext success events.
     *
     * @param {String} msg
     * @param {Object} tasks
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamSuccess = function(msg, data) {
        if (msg) {
            if (data) {
                data.schedulerId = this.schedulerId;
            }
            logger.debug(msg, data);
        }
        return Rx.Observable.empty();
    };

    /**
     * Log handler for observable onError failure events.
     *
     * @param {String} msg
     * @param {Object} err
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            schedulerId: this.schedulerId,
            // stacks on some error objects (particularly from the assert library)
            // don't get printed if part of the error object so separate them out here.
            error: _.omit(err, 'stack'),
            stack: err.stack
        });
        return Rx.Observable.empty();
    };

    /**
     * Log handler for debug messaging during development/debugging. Only
     * works when this.debug is set to true;
     *
     * @param {String} msg
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamDebug = function(msg, data) {
        if (this.debug) {
            if (data) {
                data.schedulerId = this.schedulerId;
            }
            logger.debug(msg, data);
        }
    };

    /**
     * Receive messenger events for when tasks finish, and kick off the task/graph
     * evaluation via the evaluateTaskStream.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.subscribeTaskFinished = function() {
        var self = this;
        return taskMessenger.subscribeTaskFinished(
            this.domain,
            function(data) {
                logger.debug('Listener received task finished event, triggering evaluation', {
                    data: data,
                    schedulerId: self.schedulerId
                });
                self.evaluateTaskStream.onNext(data);
            }
        );
    };

    /**
     * Publish a graph finished event with the messenger.
     *
     * @param {Object} graph
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.publishGraphFinished = function(graph) {
        return eventsProtocol.publishGraphFinished(graph.instanceId, graph._status)
        .catch(function(error) {
            logger.error('Error publishing graph finished event', {
                graphId: graph.instanceId,
                _status: graph._status,
                error: error
            });
        });
    };

    /**
     * Publish a run task event with the messenger, to be picked up by any task runners
     * within the domain.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.publishScheduleTaskEvent = function(data) {
        // TODO: Add more scheduling logic here when necessary
        return taskMessenger.publishRunTask(this.domain, data.taskId, data.graphId)
        .then(function() {
            return data;
        });
    };

    /**
     * On the case of messenger or scheduler failures, or lossiness caused by high load,
     * poll the database on an interval to pick up dropped work related to
     * updating unevaluated tasks and dependent tasks.
     *
     * @param {Object} evaluateTaskStream
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createUnevaluatedTaskPollerSubscription = function(evaluateTaskStream) {
        var self = this;

        return Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .map(self.findUnevaluatedTasks.bind(self, self.domain))
        .mergeLossy(self.concurrencyMaximums.findUnevaluatedTasks)
        .flatMap(function(tasks) { return Rx.Observable.from(tasks); })
        .map(evaluateTaskStream.onNext.bind(evaluateTaskStream));
    };

    /**
     * On the case of messenger or scheduler failures, or lossiness caused by high load,
     * poll the database on an interval to pick up dropped work related to
     * evaluating graph states and scheduling new tasks.
     *
     * @param {Object} evaluateGraphStream
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createEvaluatedTaskPollerSubscription = function(evaluateGraphStream) {
        var self = this;

        return Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .map(evaluateGraphStream.onNext.bind(evaluateGraphStream, {}));
    };

    /**
     * Find all tasks in the database that haven't been fully evaluated
     * (see TaskScheduler.prototype.updateTaskDependencies).
     *
     * @param {String} domain
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.findUnevaluatedTasks = function(domain) {
        return Rx.Observable.just()
        .flatMap(store.findUnevaluatedTasks.bind(store, domain, this.findUnevaluatedTasksLimit))
        .tap(function(tasks) {
            if (tasks && tasks.length) {
                logger.debug('Poller is triggering unevaluated tasks to be evaluated', {
                    tasks: _.map(tasks, 'taskId')
                });
            }
        })
        .catch(this.handleStreamError.bind(this, 'Error finding unevaluated tasks'));
    };

    /**
     * Subscribe to messenger events to run new graphs.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.subscribeRunTaskGraph = function() {
        return taskMessenger.subscribeRunTaskGraph(this.domain,
                this.runTaskGraphCallback.bind(this));
    };

    /**
     * Emit a new graph evaluation event to trigger TaskScheduler.prototype.findReadyTasks.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.runTaskGraphCallback = function(data) {
        assert.object(data);
        assert.uuid(data.graphId);
        this.evaluateGraphStream.onNext(data);
    };

    /**
     * Start the task scheduler and its observable pipeline, as well as the expired lease poller.
     * Subscribe to messenger events.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.start = function() {
        var self = this;
        return Promise.resolve()
        .then(function() {
            self.running = true;
            self.initializePipeline();
            self.leasePoller = LeaseExpirationPoller.create(self, {});
            self.leasePoller.start();
            return [
                self.subscribeRunTaskGraph(),
                self.subscribeTaskFinished(),
                self.subscribeCancelGraph()
            ];
        })
        .spread(function(runTaskGraphSubscription, taskFinishedSubscription) {
            self.subscriptions.push(runTaskGraphSubscription);
            self.subscriptions.push(taskFinishedSubscription);
            logger.info('Task scheduler started', {
                schedulerId: self.schedulerId,
                domain: self.domain
            });
        });
    };

    /**
     * Clean up any messenger subscriptions. Stop polling for expired leases.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.stop = function() {
        var self = this;
        self.running = false;
        if (self.leasePoller) {
            self.leasePoller.stop();
        }
        return Promise.map(this.subscriptions, function() {
            return self.subscriptions.pop().dispose();
        });
    };

    /**
     * @param {Object} options
     * @memberOf TaskScheduler
     */
    TaskScheduler.create = function(options) {
        return new TaskScheduler(options);
    };

    return TaskScheduler;
}
