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
        'Errors'
    )
);

function mongoStoreFactory(waterline, Promise, Constants, Errors) {
    function getTask(injectableName) {
        return waterline.taskdefinitions.findOne({ injectableName: injectableName })
        .then(function(taskDefinition) {
            if (!taskDefinition) {
                // TODO: for testing, remove BBP
                if (injectableName === 'Task.Base.noop') {
                    return {
                        friendlyName: 'base-noop',
                        injectableName: 'Task.Base.noop',
                        runJob: 'Job.noop',
                        requiredOptions: [
                            'option1',
                            'option2',
                            'option3',
                        ],
                        requiredProperties: {},
                        properties: {
                            noop: {
                                type: 'null'
                            }
                        }
                    };
                }
                throw new Errors.NotFoundError(
                    'Could not find task definition with injectableName %s'
                    .format(injectableName));
            } else {
                return taskDefinition.toJSON();
            }
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

    function persistTaskDependencies(task) {
        var obj = {
            instanceId: task.instanceId,
            state: Constants.TaskStates.Pending,
            dependencies: task.waitingOn
        };

        return waterline.taskdependencies.create(obj);
    }

    function findReadyTasksForGraph(graphId) {
        // TODO: Implement
        graphId;
        console.log('BBP STORE findReadyTasksForGraph');
        return Promise.resolve([]);
    }

    function checkoutTask(graphId) {
        // TODO: Implement
        graphId;
        console.log('BBP STORE checkoutTask');
    }

    function checkGraphDone(graphId) {
        /*
        var query = {
            instanceId: graphId,
            state: 'valid',
            tasks: {
                // TODO: Handle tasks that will be unreachable
                // based on separate success/failure waitOn dependencies
                // for different tasks
                state: { $in: Constants.FinishedTaskStates }
            }
        };

        return waterline.graphobjects.findOneMongo(query).then(Boolean);
        */
        // TODO: Implement
        console.log('BBP STORE checkGraphDone');
        graphId;
        return Promise.resolve(null);
    }

    return {
        getTask: getTask,
        persistGraphObject: persistGraphObject,
        persistGraphDefinition: persistGraphDefinition,
        persistTaskDefinition: persistTaskDefinition,
        getGraphDefinitions: getGraphDefinitions,
        getTaskDefinitions: getTaskDefinitions,
        persistTaskDependencies: persistTaskDependencies,
        findReadyTasksForGraph: findReadyTasksForGraph,
        checkoutTask: checkoutTask,
        checkGraphDone: checkGraphDone
    };
}