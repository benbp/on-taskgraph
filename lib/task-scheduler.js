// Copyright 2015, EMC, Inc.
//
'use strict';

var di = require('di');

module.exports = taskSchedulerFactory;
di.annotate(taskSchedulerFactory, new di.Provide('TaskGraph.TaskScheduler'));
di.annotate(taskSchedulerFactory,
    new di.Inject(
        'Protocol.Events',
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

    // TODO: use nested DB queries here to determine if any tasks are
    // waiting on failed states of any already-failed tasks
    TaskScheduler.prototype.evaluateGraphStateHandler = function(graph) {
        var self = this;
        return _.filter(graph.tasks, function(task) {
            if (task.state !== Constants.TaskStates.Pending) {
                return false;
            }
            return self.isTaskReady(task, graph);
        });
    };

    // TODO: refactor and change this strategy: pop waitingOn items off
    // the document itself when they are fulfilled, that way it's easier
    // to check if a task is ready by just seeing if waitingOn is empty.
    // NOTE: be careful of recursive loops, if the scheduler changes the doc
    // which causes a changed event, which could cause the scheduler to change
    // the doc again, and so on.
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

    // This is a modified implementation of Tarjan's topological sorting algorithm
    // (https://en.wikipedia.org/wiki/Topological_sorting#Tarjan.27s_algorithm).
    // The modifications account for layered orders for parallel execution,
    // peculiarities in our data structure and valid dependency options
    // (specific task state and/or context value added).
    //
    // See TaskScheduler.prototype.topologicalSortTasks for the entry point to
    // this function.
    TaskScheduler.prototype._topoSortVisit = function(
            taskId, graph, context, parentTaskName, sorted, layer) {
        var self = this;
        var task = graph.tasks[taskId];
        if (task.temporaryMark) {
            throw new Error('Detected a cyclic graph with tasks %s and %s'.format(
                                task.injectableName, parentTaskName));
        } else if (task.permanentMark) {
            // Tell the dependant task that it is a layer above this one in the sort.
            return task.layer + 1;
        }
        // If the task is finished, do not add it to the sorted array, and tell
        // the calling task, if there is one, that it is layer 0 as far
        // as this dependency is concerned.
        if (_.contains(Constants.FinishedTaskStates, task.state)) {
            return 0;
        }
        task.temporaryMark = true;
        // Calculate the depth at which our current task is located on the
        // sorted output, so we can insert at the right layer regardless of
        // which graph node we started the sort with.
        var depth = _.max(_.map(task.waitingOn, function(value, dep) {
            if (!_.has(graph.tasks, dep)) {
                throw new Error('Graph does not contain task with ID ' + dep);
            }
            // Do not increase the layer depth for tasks dependencies that
            // have reached a desired state.
            if (value === 'finished' && _.contains(Constants.FinishedTaskStates, value)) {
                return 0;
            } else if (graph.tasks[dep].state === value) {
                return 0;
            }
            // If a context value is a dependency, increase the layer depth
            // by the minimum amount, unless the value is there, in which case
            // do not increase the layer depth.
            // TODO: Account for context depends/provides in tasks to provide
            // more accurate layer sorting.
            if (dep === '$context') {
                return _.has(context, value) ? 0 : 1;
            }
            // TODO: account for context
            return self._topoSortVisit(dep, graph, context, task.injectableName, sorted, layer + 1);
        }));
        // Lodash _.max returns -Infinity for an empty array
        if (depth === -Infinity) {
            depth = 0;
        }
        task.permanentMark = true;
        task.temporaryMark = false;
        task.layer = depth;
        if (sorted.length - 1 < task.layer) {
            sorted.push([]);
        }
        sorted[task.layer].push(taskId);
        // Tell the dependant task that it is a layer above this one in the sort.
        return task.layer + 1;
    };

    // In order to achieve optimal execution of parallel task running based
    // on certain dependency graphs, this topological sort must be run EVERY
    // time a new dependency state is reached (task completion or context value
    // added). This ensures that if we have two layers of parallel tasks (each
    // Nth element of the second layer waiting only on the Nth element of the
    // first layer) we won't wait for all tasks in the first layer to finish
    // before starting any tasks in the second layer. Re-sorting promotes any tasks
    // with satisfied dependencies in that second layer to the first, for example:
    //
    // (arrows ^ mean a waitingOn dependency)
    // --- Start: ---
    // [A, B] <- Running
    //  ^  ^
    // [C, D]
    // --- A finishes, re-sort: ---
    // [A] <- Done
    // [B, C]
    //  ^
    // [D]
    //
    // This example shows that, by re-sorting when task A completes, we run
    // task C right away without needing to wait for task B, even though the
    // first sort produced task C at the same layer as task D.
    TaskScheduler.prototype.topologicalSortTasks = function(graph, context) {
        var self = this;
        var sorted = [];
        _.forEach(graph.tasks, function(task, taskId) {
            self._topoSortVisit(taskId, graph, context, null, sorted, 0);
        });
        return sorted;
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
