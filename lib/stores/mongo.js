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
        '_'
    )
);

function mongoStoreFactory(waterline, Promise, Constants, Errors, _) {
    // TODO: What is this?
    function setGraphDone(graphId) {
        var query = {
            _status: ''
        };
        query;

        return waterline.graphobjects.findAndModifyMongo({ instanceId: graphId })
        .then(function(graph) {
            if (_.isEmpty(graph)) {
                throw new Errors.NotFoundError(
                    'Could not find task definition with instanceId %s'
                    .format(graphId));
            }

            return graph.toJSON();
        });
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
            fields: {'instanceId': 1}
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

    function getTaskById(IDs) {
        var query = {
            instanceId: IDs.graphId
        };
        var options = {
            fields: { tasks: {} }
        };

        options.fields.tasks[IDs.instanceId] = 1;
        return waterline.graphobjects.findOne(query)
        .then(function(graph) {
            return graph.tasks[IDs.instanceId];
        });
    }

    function heartbeatTasks(leaseId) {
        var query = {
            taskRunnerLease: leaseId
        };
        var update = {
            $set: {
                taskRunnerHeartbeat: new Date()
            }
        };

        return waterline.taskdependencies.updateManyMongo(query, update, {});
    }

    function findReadyTasksForGraph(data) {
        var query = {
            graphId: data.graphId,
            dependencies: {},
            reachable: true
        };
       // console.log(graphId);
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
                'instanceId': 1,
                'graphId': 1
            }
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    }

    function checkoutTaskForRunner(leaseId, task) {
        var query = {
            graphId: task.graphId,
            instanceId: task.instanceId,
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
            $in: [taskState, Constants.TaskStates.FailedTaskStates]
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

    function checkGraphDone(graphId) {
        var query = {
            graphId: graphId,
            state: Constants.TaskStates.Pending,
            reachable: true
        };

        return waterline.taskdependencies.findOne(query)
        .then(function(result) {
            if (_.isEmpty(result)) {
                return graphId;
            } else {
                return null;
            }
        });
    }

    function updateDependentTasks(data) {
        var query = {
            graphId: data.graphId,
            reachable: true
        };
        query['dependencies.' + data.taskId] = {
            $in: [data.taskState, Constants.TaskStates.FinishedTaskStates]
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
        var query = {
            graphId: data.graphId,
        };
        query['dependencies.' + data.taskId] = {
            $nin: [data.taskState, Constants.TaskStates.FinishedTaskStates]
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
