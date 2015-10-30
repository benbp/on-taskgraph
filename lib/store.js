// Copyright 2015, EMC, Inc.
'use strict';

var di = require('di');

module.exports = storeFactory;
di.annotate(storeFactory, new di.Provide('TaskGraph.Store'));
di.annotate(storeFactory,
    new di.Inject(
        'Services.Configuration',
        di.Injector
    )
);
function storeFactory(configuration, injector) {
    function get() {
        var mode = configuration.get('taskgraph-store', 'mongo');
        switch(mode) {
            case 'mongo':
                return injector.get('TaskGraph.Stores.Mongo');
            default:
                throw new Error('Unknown taskgraph store: ' + mode);
        }
    }

    // Defer running of get() until after startup, because in mongo mode,
    // waterline won't have model attributes until after the on-core
    // services start up.
    return {
        get: get
    };
}
