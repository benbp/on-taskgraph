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
    var exports = {};

    // NOTE: This is meant to be idempotent, and just drop the update silently
    // if the graph has already been marked as done elsewhere and the query returns
    // empty.
    exports.setGraphDone = function(state, data) {
        assert.string(state);
        assert.object(data);
        assert.string(data.graphId);

        var query = {
            instanceId: data.graphId,
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
    };

    exports.setTaskState = function(taskId, graphId, state) {
        assert.string(taskId);
        assert.string(graphId);
        assert.string(state);

        // TODO: including graphId with the intent that we'll create an
        // index against it in the database
        var query = {
            graphId: graphId,
            taskId: taskId
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
    };

    exports.getTaskDefinition = function(injectableName) {
        return waterline.taskdefinitions.findOne({ injectableName: injectableName })
        .then(function(taskDefinition) {
            if (_.isEmpty(taskDefinition)) {
                throw new Errors.NotFoundError(
                    'Could not find task definition with injectableName %s'
                    .format(injectableName));
            }

            return taskDefinition.toJSON();
        });
    };

    exports.persistGraphDefinition = function(definition) {
        var query = {
            injectableName: definition.injectableName
        };
        var options = {
            new: true,
            upsert: true
        };

        return waterline.graphdefinitions.findAndModifyMongo(query, {}, definition, options);
    };

    exports.persistTaskDefinition = function(definition) {
        var query = {
            injectableName: definition.injectableName
        };
        var options = {
            new: true,
            upsert: true
        };

        return waterline.taskdefinitions.findAndModifyMongo(query, {}, definition, options);
    };

    exports.getGraphDefinitions = function() {
        return waterline.graphdefinitions.find({});
    };

    exports.getTaskDefinitions = function() {
        return waterline.taskdefinitions.find({});
    };

    exports.persistGraphObject = function(graph) {
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
    };

    exports.persistTaskDependencies = function(taskDependencyItem, graphId) {
        var obj = {
            taskId: taskDependencyItem.taskId,
            graphId: graphId,
            state: Constants.TaskStates.Pending,
            dependencies: taskDependencyItem.dependencies
        };

        return waterline.taskdependencies.create(obj);
    };

    exports.getTaskById = function(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

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
        options.fields.tasks[data.taskId] = 1;

        return waterline.graphobjects.findOne(query)
        .then(function(graph) {
            return {
                graphId: graph.instanceId,
                context: graph.context,
                task: graph.tasks[data.taskId]
            };
        });
    };

    // TODO: it probably makes more sense to have a separate collection
    // or just use AMQP for heartbeating, and then when a heartbeat for a
    // runner ID is dropped, only then will services do the query to check out
    // all its tasks.
    exports.heartbeatTasks = function(leaseId) {
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
    };

    exports.findActiveGraphs = function(domain) {
        assert.string(domain);

        var query = {
            domain: domain,
            _status: Constants.TaskStates.Pending
        };

        return waterline.graphobjects.find(query);
    };

    exports.findUnevaluatedTasks = function(schedulerId, domain) {
        assert.string(schedulerId);
        assert.string(domain);

        var query = {
            schedulerLease: {
                $in: [schedulerId, null]
            },
            domain: domain,
            evaluated: false,
            reachable: true,
            state: {
                $in: Constants.FinishedTaskStates.concat([Constants.TaskStates.Finished])
            }
        };

        return waterline.taskdependencies.find(query);
    };

    exports.findReadyTasks = function(schedulerId, domain, graphId) {
        assert.string(schedulerId);
        assert.string(domain);
        if (graphId) {
            assert.string(graphId);
        }

        var query = {
            schedulerLease: {
                $in: [schedulerId, null]
            },
            taskRunnerLease: null,
            domain: domain,
            dependencies: {},
            reachable: true,
            state: Constants.TaskStates.Pending
        };
        if (graphId) {
            query.graphId = graphId;
        }

        return waterline.taskdependencies.find(query)
        .then(function(tasks) {
            return {
                tasks: tasks,
                graphId: graphId || null
            };
        });
    };

    exports.checkoutTaskForScheduler = function(schedulerId, domain, data) {
        assert.string(schedulerId);
        assert.string(domain);
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            taskId: data.taskId,
            schedulerLease: {
                $in: [schedulerId, null]
            },
            taskRunnerLease: null
        };
        var update = {
            $set: {
                schedulerLease: schedulerId,
                schedulerHeartbeat: new Date()
            }
        };
        var options = {
            new: true,
            fields: {
                _id: 0,
                taskId: 1,
                graphId: 1
            }
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    };

    exports.checkoutTaskForRunner = function(taskRunnerId, data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            taskId: data.taskId,
            taskRunnerLease: null,
        };
        var update = {
            $set: {
                taskRunnerLease: taskRunnerId,
                taskRunnerHeartbeat: new Date()
            }
        };
        var options = {
            new: true
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    };

    exports.isTaskFailureHandled = function(graphId, taskId, taskState) {
        assert.string(graphId);
        assert.string(taskId);
        assert.string(taskState);

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
    };

    exports.checkGraphFinished = function(data) {
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
    };

    exports.updateDependentTasks = function(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);
        assert.string(data.state);

        var query = {
            graphId: data.graphId,
            reachable: true
        };
        query['dependencies.' + data.taskId] = {
            $in: [data.state, Constants.TaskStates.Finished]
        };
        var update = {
            $unset: {}
        };
        update.$unset['dependencies.' + data.taskId] = '';
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options);
    };

    exports.updateUnreachableTasks = function(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);
        assert.string(data.state);

        var query = {
            graphId: data.graphId,
        };
        query['dependencies.' + data.taskId] = {
            $in: _.difference(Constants.FinishedTaskStates, [data.state])
        };
        var update = {
            $set: {
                reachable: false
            }
        };
        var options = {
            multi: true
        };

        return waterline.taskdependencies.updateMongo(query, update, options);
    };

    exports.markTaskEvaluated = function(data) {
        assert.object(data);
        assert.string(data.graphId);
        assert.string(data.taskId);

        var query = {
            graphId: data.graphId,
            taskId: data.taskId
        };
        var update = {
            $set: {
                evaluated: true
            }
        };
        var options = {
            new: true
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    };

    exports.findExpiredSchedulerLeases = function(domain, leaseAdjust) {
        assert.string(domain);
        assert.number(leaseAdjust);

        var query = {
            domain: domain,
            schedulerHeartbeat: {
                $lt: new Date(Date.now() - leaseAdjust)
            }
        };

        return waterline.taskdependencies.find(query);
    };

    exports.expireSchedulerLease = function(objId) {
        assert.string(objId);

        var query = {
            _id: waterline.taskdependencies.mongo.objectId(objId)
        };
        var update = {
            $set: {
                schedulerLease: null,
                schedulerHeartbeat: null
            }
        };
        var options = {
            new: true
        };

        return waterline.taskdependencies.findAndModifyMongo(query, {}, update, options);
    };

    return exports;
}
