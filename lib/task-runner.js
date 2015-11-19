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

    function TaskRunner(options) {
        options = options || {};
        this.taskRunnerId = uuid.v4();
        this.runTaskStream = new Rx.Subject();
        this.pipelines = null;
        this.heartbeatInterval = options.heartbeatInterval || 2000;
        this.running = false;
        this.activeTasks = {};
        this.domain = options.domain || 'default';
    }

    TaskRunner.prototype.isRunning = function() {
        return this.running;
    };

    TaskRunner.prototype.initializePipeline = function() {
        var self = this;
        var runTaskSubscription = self.createRunTaskSubscription(self.runTaskStream);
        var heartbeatSubscription = self.createHeartbeatSubscription();

        return [
            runTaskSubscription,
            heartbeatSubscription
        ];
    };

    // TODO: refactor this into an Observable stream rather than a promise chain
    TaskRunner.prototype.runTask = function(data) {
        var self = this;

        return Promise.resolve()
        .then(function() {
            var task = Task.create(data.task, { instanceId: data.task.instanceId }, data.context);
            logger.debug("Running task ", {
                taskRunnerId: self.taskRunnerId,
                taskId: task.instanceId,
                taskName: task.definition.injectableName
            });
            self.activeTasks[task.instanceId] = task;
            return task.run();
        })
        .then(function(task) {
            return [
                task,
                store.setTaskState(task.instanceId, data.graphId, task.state)
            ];
        })
        .spread(function(task) {
            return taskMessenger.publishTaskFinished(
                self.domain, task.instanceId, data.graphId, task.state);
        })
        .catch(function(error) {
            // TODO: the above chain to set task state and publish task finished
            // should happen in both success and failure cases.
            // Do this work when I refactor this whole chunk to an observable
            // stream.
            logger.error('Task failure', {
                taskRunnerId: self.taskRunnerId,
                taskId: data.task.instanceId,
                error: error
            });
        })
        .finally(function() {
            delete self.activeTasks[data.task.instanceId];
        });
    };

    TaskRunner.prototype.subscribeRunTask = function() {
        return taskMessenger.subscribeRunTask(
                this.domain,
                this.runTaskStream.onNext.bind(this.runTaskStream)
            );
    };

    TaskRunner.prototype.createRunTaskSubscription = function(runTaskStream) {
        var self = this;
        return runTaskStream
            .takeWhile(self.isRunning.bind(self))
            .flatMap(store.checkoutTaskForRunner.bind(store, self.taskRunnerId))
            .filter(function(task) { return !_.isEmpty(task); })
            .flatMap(store.getTaskById.bind(store))
            .subscribe(
                self.runTask.bind(self),
                self.handleStreamError.bind(self, 'Failed to run task')
            );
    };

    TaskRunner.prototype.createHeartbeatSubscription = function() {
        var self = this;
        return Rx.Observable.interval(self.heartbeatInterval)
                .takeWhile(self.isRunning.bind(self))
                .flatMap(store.heartbeatTasks.bind(self, self.taskRunnerId))
                .catch(function(error) {
                    logger.error('Failed to update heartbeat, stopping task runner and tasks', {
                        taskRunnerId: self.taskRunnerId,
                        error: error,
                        activeTasks: _.keys(self.activeTasks)
                    });
                    return Rx.Observable.flatMap(function() {
                        return self.stop();
                    });
                })
                .subscribe(
                    self.handleStreamSuccess.bind(self, null),
                    self.handleStreamError.bind(self, 'Error handling task runner heartbeat')
                );
    };

    TaskRunner.prototype.handleStreamSuccess = function(msg, data) {
        if (msg) {
            if (data && !data.taskRunnerId) {
                data.taskRunnerId = this.taskRunnerId;
            }
            logger.debug(msg, data);
        }
        return Rx.Observable.empty();
    };

    TaskRunner.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            taskRunnerId: this.taskRunnerId,
            error: err
        });
        return Rx.Observable.empty();
    };

    TaskRunner.prototype.stopHeartbeat = function() {
        clearInterval(this.heartbeat);
    };

    TaskRunner.prototype.stop = function() {
        try {
            this.running = false;
            while (!_.isEmpty(this.pipelines)) {
                this.pipelines.pop().dispose();
            }
        } catch (e) {
            logger.error('Failed to stop task runner', {
                taskRunnerId: this.taskRunnerId,
                error: e
            });
        }
    };

    TaskRunner.prototype.start = function() {
        var self = this;
        return Promise.resolve()
        .then(function() {
            self.running = true;
            self.pipelines = self.initializePipeline();
            //var now = new Date();
            //return taskMessenger.subscribe({
            //        domain: this.domain,
            //        createdAt: {$gt: now}
            //    }, 'taskevents', null, console.log);
            return self.subscribeRunTask();
        });
    };

    TaskRunner.create = function() {
        return new TaskRunner();
    };

    return TaskRunner;
}
