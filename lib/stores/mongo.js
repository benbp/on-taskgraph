// Copyright 2015, EMC, Inc.
'use strict';

var di = require('di');

module.exports = mongoStoreFactory;
di.annotate(mongoStoreFactory, new di.Provide('TaskGraph.Stores.Mongo'));
di.annotate(mongoStoreFactory,
    new di.Inject(
        'Services.Waterline',
        'Logger',
        'Errors'
    )
);

function mongoStoreFactory(waterline, Logger, Errors) {
    var logger = Logger.initialize(mongoStoreFactory);

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

    function persistGraphObject(graph) {
        // TODO: Implement
        debugger;
    }

    function persistDependencies(task) {
        debugger;
        console.log('BBP TASK DEPS ' + task.injectableName);
    }

    function findReadyTasksForGraph(graphId) {
        console.log('BBP STORE findReadyTasksForGraph');
    }

    function checkoutTask(graphId) {
        console.log('BBP STORE checkoutTask');
    }

    return {
        getTask: getTask,
        persistGraphObject: persistGraphObject,
        persistDependencies: persistDependencies,
        findReadyTasksForGraph: findReadyTasksForGraph,
        checkoutTask: checkoutTask
    };
}
