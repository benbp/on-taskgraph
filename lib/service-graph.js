// Copyright 2015-2016, EMC, Inc.

'use strict';

var di = require('di');

module.exports = ServiceGraph;

di.annotate(ServiceGraph, new di.Provide('TaskGraph.ServiceGraph'));
di.annotate(ServiceGraph, new di.Inject(
        'TaskGraph.TaskGraph',
        'TaskGraph.Store',
        'Protocol.TaskGraphRunner',
        'Constants',
        'Promise',
        '_'
    )
);
function ServiceGraph(TaskGraph, store, taskGraphProtocol, Constants, Promise, _) {
    function createAndRunServiceGraph(definition, domain) {
        return TaskGraph.create(domain, { definition: definition })
        .then(function(graph) {
            return graph.persist();
        })
        .then(function(graph) {
            return taskGraphProtocol.runTaskGraph(graph.instanceId);
        });
    }

    function start(domain) {
        domain = domain || Constants.DefaultTaskDomain;

        return Promise.all([store.getGraphDefinitions(), store.getServiceGraphs()])
        .spread(function(graphDefinitions, serviceGraphs) {
            var serviceGraphDefinitions = _.filter(graphDefinitions, function(graph) {
                return graph.serviceGraph;
            });

            var groups = _.transform(serviceGraphs, function(result, graph) {
                if (_.contains(Constants.FailedTaskStates, graph._status)) {
                    result.failed[graph.injectableName] = graph.instanceId;
                } else if (_.contains(Constants.TaskStates.Pending, graph._status)) {
                    result.running.push(graph.injectableName);
                }
            }, { failed: {}, running: [] });

            return Promise.map(serviceGraphDefinitions, function(def) {
                if (_.contains(groups.running, def.injectableName)) {
                    return;
                }
                if (_.contains(_.keys(groups.failed), def.injectableName)) {
                    return store.deleteGraph(groups.failed[def.injectableName])
                    .then(function() {
                        return createAndRunServiceGraph(def, domain);
                    });
                }
                return createAndRunServiceGraph(def, domain);
            });
        });
    }

    function stop() {
        return store.getServiceGraphs()
        .then(function(graphs) {
            return Promise.map(graphs, function(graph) {
                return taskGraphProtocol.cancelTaskGraph(graph.instanceId);
            });
        });
    }

    return {
        _createAndRunServiceGraph: createAndRunServiceGraph,
        start: start,
        stop: stop
    };
}
