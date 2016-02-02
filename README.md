# on-taskgraph [![Build Status](https://travis-ci.org/RackHD/on-taskgraph.svg?branch=master)](https://travis-ci.org/RackHD/on-taskgraph) [![Code Climate](https://codeclimate.com/github/RackHD/on-taskgraph/badges/gpa.svg)](https://codeclimate.com/github/RackHD/on-taskgraph) [![Coverage Status](https://coveralls.io/repos/RackHD/on-taskgraph/badge.svg?branch=master&service=github)](https://coveralls.io/github/RackHD/on-taskgraph?branch=master)

'on-taskgraph' is the core workflow engine for RackHD, initiating workflows, performing tasks, and responding to ancillary services to enable the RackHD service

Copyright 2015-2016, EMC, Inc.

## installation/setup

    npm install

    # For Linux
    apt-get install mongodb
    apt-get install rabbitmq-server

    # If on OSX, see http://brew.sh/ if brew is not installed
    brew install mongodb26
    brew install rabbitmq

**it is highly recommended to create indexes in mongo to reduce the CPU footprint and increase performance**

```
$ mongo pxe
> db.createCollection('taskdependencies')
> db.createCollection('graphobjects')
> db.taskdependencies.createIndex({ taskId: 1 })
> db.taskdependencies.createIndex({ graphId: 1 })
> db.taskdependencies.createIndex({ graphId: 1, state: 1 })
```

## running

*NOTE: requires RabbitMQ and Mongo to be running to start and test correctly.*

This project can be run in one of three possible modes, [scheduler mode](#scheduler-mode), [task runner mode](#task-runner-mode), and [hybrid mode](#hybrid-mode):

If no mode is specified, it will run in [hybrid mode](#hybrid-mode) by default:

    node index.js

To run in [scheduler mode](#scheduler-mode):

    node index.js --scheduler
    # OR
    node index.js -s

To run in [task runner mode](#task-runner-mode):

    node index.js --runner
    # OR
    node index.js -r


**Which mode should I run in?**

First, read the [descriptions of each mode](#modes) below. Here are some guidelines for making this decision:

*If you have no concerns about fault tolerance or high performance. You are a normal user.*

- Run a single process in hybrid mode

*If you want to optimize for performance and speedy processing of a large volume of workflows*

- Run one process in Scheduler mode, and multiple processes in Task Runner mode. Make sure you create mongo indexes as detailed [above](#installationsetup).

*If you want to optimize for high availability*

- Run two processes in Scheduler mode (potentially on different machines) and run multiple processes in Task Runner mode (again distributing across machines if desired).

To interact with the system externally, e.g. running graphs against live systems, you need to be running the following RackHD services:

- [on-http](https://github.com/RackHD/on-http/) (mandatory for all)
- [on-dhcp-proxy](https://github.com/RackHD/on-dhcp-proxy/) (mandatory for node graphs)
- [on-tftp](https://github.com/RackHD/on-tftp/) (mandatory for node booting graphs)

In addition, the system requires MongoDB, ISC DHCP, and RabbitMQ to be running and configured appropriately to enable PXE booting. More information is
available in the documentation for RackHD at http://rackhd.readthedocs.org

## Overview

This repository provides functionality for running encapsulated jobs/units of work via
graph-based control flow mechanisms. For example, a typical graph consists of a list of
tasks, which themselves are essentially decorated functions. The graph definition specifies
any context and/or option values that should be handed to these functions, and more importantly,
it provides a mechanism for specifying when each task should be run. The most simple case is
saying a task should be run only after a previous task has succeeded (essentially becoming a
state machine). More complex graphs may involve event based task running, or defining
data/event channels that should exist between concurrently running tasks.

HA/Fault tolerance

Atomic checkout: All eligible Schedulers or Task Runners will receive requests, but only one will succeed in checking out a lease to handle that request. Somewhat like a leased queue model. Leverage existing database technologies (currently MongoDB)
Lease heartbeat: Workflow engine instances heartbeat their owned tasks, so that other instances can check them out on timed out heartbeats.
Backup mechanisms for dropped events: Utilize optimized pollers to queue up dropped events for re-evaluation (dropped events happen under high load throttling and catastrophic failure conditons).

Scalability

Domains: Workflow instances can be configured to handle different domains of tasks, and can be machine independentas long as the database and messaging are shared.
Stateless: Horizontal scalability is achieved by designing the processes to run in essentially a stateless mode. The last word is from the database.
Optimize data structure: update the current data structures and mongo collections/indexes to be optimized for fast querying, improved indexing

## Modes

### Scheduler mode

In Scheduler mode the process will only take responsibility for evaluating workflow graphs and queuing tasks to be run by task runners.

### Task Runner mode

In Task Runner mode the process will listen/poll for queued tasks, and check them out to be run. It is in Task Runner mode that the the actual job code is executed.

### Hybrid mode

## API commands

When running the on-http process, these are some common API commands you can send:

**Get available graphs**

```
GET
/api/common/workflows/library
```

**Run a new graph against a node**

Find the graph definition you would like to use, and copy the top-level *injectableName* attribute

```
POST
/api/common/nodes/<id>/workflows
{
    name: <graph name>
}
```

**Run a new graph not linked to a node**

Find the graph definition you would like to use, and copy the top-level *injectableName* attribute

```
POST
/api/common/workflows
{
    name: <graph name>
}
```

This will return a serialized graph object.

**Query an active graph's state**

```
GET
/api/common/nodes/<id>/workflows/active
```

**Create a new graph definition**

```
PUT
/api/common/workflows
{
    <json definition of graph>
}
```

### Creating new graphs

Graphs are defined via a JSON definition that conform to this schema:

- friendlyName (string): a human readable name for the graph
- injectableName (string): a unique name used by the system and the API to refer to the graph
- tasks (array of objects): a list of task definitions or references to task definitions. For an in-depth explanation
        of task definitions, see [the on-tasks README](https://hwstashprd01.isus.emc.com:8443/projects/ONRACK/repos/on-tasks/browse/README.md)
    - tasks.label (string): a unique string to be used as a reference within the graph definition
    - tasks.\[taskName\] (string): the injectableName of a task in the database to run. This or taskDefinition is required.
    - tasks.\[taskDefinition\] (object): an inline definition of a task, instead of one in the database. This or taskName is required.
    - tasks.\[ignoreFailure\] (boolean): ignoreFailure: true will prevent the graph from failing on task failure
    - tasks.\[waitOn\] (object): key, value pairs referencing other task labels to desired states of those tasks to trigger running on.
                                    Available states are 'succeeded', and 'failed' and 'finished' (run on succeeded or failed). If waitOn
                                    is not specified, the task will run on graph start.
- [options]
    - options.\[defaults\] (object): key, value pairs that will be handed to any tasks that have matching option keys
    - options.\<label\> (object): key, value pairs that should all be handed to a specific task


## Debugging/Profiling

To run in debug mode:

    sudo node debug index.js

If you're using Node v4 or greater you can use `node-inspector` to debug and profile from a GUI.

    npm install node-inspector -g
    node-inspector --preload=false &
    sudo node --debug-brk index.js

Note: do not use the `node-debug` command it doesn't work as well.

## CI/testing

To run tests from a developer console:

    npm test

To run tests and get coverage for CI:

    # verify hint/style
    ./node_modules/.bin/jshint -c .jshintrc --reporter=checkstyle lib index.js > checkstyle-result.xml || true
    ./node_modules/.bin/istanbul cover -x "**/spec/**" _mocha -- $(find spec -name '*-spec.js') -R xunit-file --require spec/helper.js
    ./node_modules/.bin/istanbul report cobertura
    # if you want HTML reports locally
    ./node_modules/.bin/istanbul report html
