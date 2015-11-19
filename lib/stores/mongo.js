// Copyright 2015, EMC, Inc.
'use strict';

var di = require('di');

module.exports = mongoStoreFactory;
di.annotate(mongoStoreFactory, new di.Provide('TaskGraph.Stores.Mongo'));
di.annotate(mongoStoreFactory,
    new di.Inject(
        'Services.Waterline',
        'Promise',
        'Constants',
        'Errors',
        'Assert',
        '_'
    )
);

function mongoStoreFactory(waterline, Promise, Constants, Errors, assert, _) {
    // NOTE: This is meant to be idempotent, and just drop the update silently
    // if the graph has already been marked as done elsewhere and the query returns
    // empty.
    function setGraphDone(graphId, state) {
        var query = {
            instanceId: graphId,
            _status: Constants.TaskStates.Pending
        };
        var update = {
            $set: {
                _status: state
            }
        };
        var options = {
            new: true
        };

        return waterline.graphobjects.findAndModifyMongo(query, {}, update, options);
    }

    function setTaskState(taskId, graphId, state) {
        // TODO: including graphId with the intent that we'll create an
        // index against it in the database
        var query = {
            graphId: graphId,
            instanceId: taskId
        };
        var update = {
            $set: {
                state: state
            }
        };
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options);
    }

    function getTaskDefinition(injectableName) {
        return waterline.taskdefinitions.findOne({ injectableName: injectableName })
        .then(function(taskDefinition) {
            if (_.isEmpty(taskDefinition)) {
                throw new Errors.NotFoundError(
                    'Could not find task definition with injectableName %s'
                    .format(injectableName));
            }

            return taskDefinition.toJSON();
        });
    }

    function persistGraphDefinition(definition) {
        var query = {
            injectableName: definition.injectableName
        };
        var options = {
            new: true,
            upsert: true
        };

        return waterline.graphdefinitions.findAndModifyMongo(query, {}, definition, options);
    }

    function persistTaskDefinition(definition) {
        var query = {
            injectableName: definition.injectableName
        };
        var options = {
            new: true,
            upsert: true
        };

        return waterline.taskdefinitions.findAndModifyMongo(query, {}, definition, options);
    }

    function getGraphDefinitions() {
        return waterline.graphdefinitions.find({});
    }

    function getTaskDefinitions() {
        return waterline.taskdefinitions.find({});
    }

    function persistGraphObject(graph) {
        var query = {
            instanceId: graph.instanceId
        };
        var options = {
            new: true,
            upsert: true,
            fields: {
                _id: 0,
                instanceId: 1
            }
        };

        return waterline.graphobjects.findAndModifyMongo(query, {}, graph, options);
    }

    function persistTaskDependencies(taskDependencyItem, graphId) {
        var obj = {
            instanceId: taskDependencyItem.taskId,
            graphId: graphId,
            state: Constants.TaskStates.Pending,
            dependencies: taskDependencyItem.dependencies
        };

        return waterline.taskdependencies.create(obj);
    }

    function getTaskById(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.instanceId);

        var query = {
            instanceId: data.graphId
        };
        var options = {
            fields: {
                _id: 0,
                instanceId: 1,
                context: 1,
                tasks: {}
            }
        };
        options.fields.tasks[data.instanceId] = 1;

        return waterline.graphobjects.findOne(query)
        .then(function(graph) {
            return {
                graphId: graph.instanceId,
                context: graph.context,
                task: graph.tasks[data.instanceId]
            };
        });
    }

    // TODO: it probably makes more sense to have a separate collection
    // or just use AMQP for heartbeating, and then when a heartbeat for a
    // runner ID is dropped, only then will services do the query to check out
    // all its tasks.
    function heartbeatTasks(leaseId) {
        var query = {
            taskRunnerLease: leaseId
        };
        var update = {
            $set: {
                taskRunnerHeartbeat: new Date()
            }
        };
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options);
    }

    function findReadyTasksForGraph(data) {
        assert.object(data);
        assert.string(data.graphId);

        var query = {
            graphId: data.graphId,
            dependencies: {},
            reachable: true,
            state: Constants.TaskStates.Pending
        };

        return waterline.taskdependencies.find(query)
        .then(function(tasks) {
            return {
                tasks: tasks,
                graphId: data.graphId
            };
        });
    }

    function checkoutTaskForScheduler(leaseId, task) {
        var query = {
            graphId: task.graphId,
            instanceId: task.instanceId,
            lease: null
        };
        var update = {
            $set: {
                schedulerLease: leaseId,
                schedulerHeartbeat: new Date()
            }
        };
        var options = {
            new: true,
            fields: {
                _id: 0,
                instanceId: 1,
                graphId: 1
            }
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    }

    function checkoutTaskForRunner(leaseId, data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            instanceId: data.taskId,
            lease: null,
        };
        var update = {
            $set: {
                taskRunnerLease: leaseId,
                taskRunnerHeartbeat: new Date()
            }
        };
        var options = {
            new: true
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    }

    function isTaskFailureHandled(graphId, taskId, taskState) {
        var query = {
            graphId: graphId,
        };
        query['dependencies.' + taskId] = {
            $in: _.union(taskState, Constants.FailedTaskStates)
        };

        // TODO: does 'a.b' syntax work in waterline queries or do we have
        // to poke a hole through to native here?
        return waterline.taskdependencies.findOneMongo(query)
        .then(function(result) {
            // TODO: will this return an array if using the native findOne?
            // with waterline findOne it will
            return Boolean(result.length);
        });
    }

    function checkGraphDone(data) {
        assert.object(data);
        assert.string(data.graphId);

        var query = {
            graphId: data.graphId,
            state: Constants.TaskStates.Pending,
            reachable: true
        };

        return waterline.taskdependencies.findOne(query)
        .then(function(result) {
            if (_.isEmpty(result)) {
                data.done = true;
            } else {
                data.done = false;
            }
            return data;
        });
    }

    function updateDependentTasks(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            reachable: true
        };
        query['dependencies.' + data.taskId] = {
            $in: [data.taskState, Constants.TaskStates.Finished]
        };
        var update = {
            $unset: {}
        };
        update.$unset['dependencies.' + data.taskId] = '';
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options)
        .then(function() {
            return data;
        });
    }

    function updateUnreachableTasks(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
        };
        query['dependencies.' + data.taskId] = {
            $in: _.difference(Constants.FinishedTaskStates, [data.taskState])
        };
        var update = {
            $set: {
                reachable: false
            }
        };
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options)
        .then(function() {
            return data;
        });
    }

    function markTaskEvaluated(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            instanceId: data.taskId
        };
        var update = {
            $set: {
                evaluated: true
            }
        };
        var options = {
            new: true
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options)
        .then(function() {
            return data;
        });
    }

    return {
        setGraphDone: setGraphDone,
        setTaskState: setTaskState,
        getTaskDefinition: getTaskDefinition,
        heartbeatTasks: heartbeatTasks,
        persistGraphObject: persistGraphObject,
        persistGraphDefinition: persistGraphDefinition,
        persistTaskDefinition: persistTaskDefinition,
        getGraphDefinitions: getGraphDefinitions,
        getTaskDefinitions: getTaskDefinitions,
        persistTaskDependencies: persistTaskDependencies,
        findReadyTasksForGraph: findReadyTasksForGraph,
        checkoutTaskForScheduler: checkoutTaskForScheduler,
        checkoutTaskForRunner: checkoutTaskForRunner,
        getTaskById: getTaskById,
        checkGraphDone: checkGraphDone,
        isTaskFailureHandled: isTaskFailureHandled,
        updateDependentTasks: updateDependentTasks,
        updateUnreachableTasks: updateUnreachableTasks,
        markTaskEvaluated: markTaskEvaluated
    };
}
