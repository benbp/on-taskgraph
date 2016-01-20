// Copyright 2015, EMC, Inc.

'use strict';

var di = require('di');

module.exports = completedTaskPollerFactory;
di.annotate(completedTaskPollerFactory,
        new di.Provide('TaskGraph.CompletedTaskPoller'));
di.annotate(completedTaskPollerFactory,
    new di.Inject(
        'TaskGraph.Store',
        'Protocol.Events',
        'Logger',
        'Assert',
        'Constants',
        'Rx',
        '_'
    )
);

function completedTaskPollerFactory(
    store,
    eventsProtocol,
    Logger,
    assert,
    Constants,
    Rx,
    _
) {
    var logger = Logger.initialize(completedTaskPollerFactory);

    function CompletedTaskPoller(domain, options) {
        options = options || {};
        assert.string(domain);
        this.running = false;
        this.pollInterval = options.pollInterval || (30 * 1000);
        this.domain = domain;
    }

    CompletedTaskPoller.prototype.pollTaskRunnerLeases = function() {
        var self = this;
        assert.ok(self.running, 'lease expiration poller is running');

        Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .flatMap(store.findCompletedTasks.bind(store))
        .flatMap(self.deleteCompletedGraphs.bind(self))
        .flatMap(self.deleteTasks.bind(self))
        .subscribe(
            self.handleStreamDebug.bind(self, 'CompletedTaskPoller stream pipeline success'),
            self.handleStreamError.bind(self, 'Error with completed task deletion stream.')
        );
    };

    CompletedTaskPoller.prototype.isRunning = function() {
        return this.running;
    };

    CompletedTaskPoller.prototype.handlePotentialFinishedGraph = function(data) {
        var self = this;
        return Rx.Observable.just(data)
        .flatMap(store.checkGraphFinished.bind(store))
        .filter(function(_data) { return _data.done; })
        .flatMap(store.setGraphDone.bind(store, Constants.TaskStates.Succeeded))
        .tap(function(graph) {
            // Don't publish duplicate events if we've already set the graph as done
            // prior, but DO continue with the outer stream so that we delete
            // the task document whose existence triggered this check.
            if (!_.isEmpty(graph)) {
                self.publishGraphFinished(graph);
            }
        });
    };

    CompletedTaskPoller.prototype.deleteCompletedGraphs = function(tasks) {
        assert.arrayOfObject(tasks);
        var terminalTasks = _(tasks).map(function(task) {
            // TODO: better to have granularity here about whether it is
            // the definitive last task, or whether there are parallel
            // last tasks, that way we can avoid unnecessary database calls to
            // check if the graph is finished.

            // Collect only terminal tasks (tasks that we know are the last or
            // one of the last tasks to run in a graph) for determing graph completion checks.
            // This logic handles cases where all tasks in a graph are completed,
            // but the graph completion event was dropped by the scheduler, either
            // due to high load or a process failure. Hooking onto task deletion
            // allows us to avoid doing full collection scans against graphobjects
            // to find potential unfinished graphs.
            if (task.terminal) {
                return task.graphId;
            }
        }).compact().uniq().value();

        return Rx.Observable.from(terminalTasks)
        .flatMap(this.handlePotentialFinishedGraph.bind(this))
        .bufferWithCount(terminalTasks.length)
        .map(tasks)
        .catch(this.handleStreamError.bind(this, 'Error handling potential finished graphs'));
    };

    CompletedTaskPoller.prototype.deleteTasks = function(tasks) {
        assert.arrayOfObject(tasks);
        var taskIds = _(tasks).map('taskId').value();

        return Rx.Observable.just(taskIds)
        .flatMap(store.deleteTasks.bind(store))
        .catch(this.handleStreamError.bind(this, 'Error deleting completed tasks'));
    };

    CompletedTaskPoller.prototype.publishGraphFinished = function(graph) {
        return eventsProtocol.publishGraphFinished(graph.instanceId, graph._status)
        .catch(function(error) {
            logger.error('Error publishing graph finished event', {
                graphId: graph.instanceId,
                _status: graph._status,
                error: error
            });
        });
    };

    CompletedTaskPoller.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            // stacks on some error objects (particularly from the assert library)
            // don't get printed if part of the error object so separate them out here.
            error: _.omit(err, 'stack'),
            stack: err.stack
        });
        return Rx.Observable.empty();
    };

    CompletedTaskPoller.prototype.handleStreamDebug = function(msg, data) {
        if (this.debug) {
            logger.debug(msg, data);
        }
    };

    CompletedTaskPoller.prototype.start = function() {
        this.running = true;
        this.pollTaskRunnerLeases();
    };

    CompletedTaskPoller.prototype.stop = function() {
        this.running = false;
    };

    CompletedTaskPoller.create = function(domain, options) {
        return new CompletedTaskPoller(domain, options);
    };

    return CompletedTaskPoller;
}
