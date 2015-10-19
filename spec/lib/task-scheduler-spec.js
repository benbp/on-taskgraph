// Copyright 2015, EMC, Inc.

'use strict';

describe('Task Scheduler', function() {
    var TaskScheduler;
    var taskScheduler;
    var Constants;

    before('before Task Scheduler', function() {
        helper.setupInjector([require('../../lib/task-scheduler')]);
        Constants = helper.injector.get('Constants');
        TaskScheduler = helper.injector.get('TaskGraph.TaskScheduler');
    });

    describe('Scheduling Pipeline', function() {
        beforeEach('beforeEach Scheduling Pipeline', function() {
            taskScheduler = TaskScheduler.create();
            taskScheduler.evaluateGraphStateHandler = sinon.stub();
            taskScheduler.evaluateExternalContextHandler = sinon.stub();
            taskScheduler.scheduleTaskHandler = sinon.stub();
            taskScheduler.initializePipeline();
        });

        it('should respond to graph state change events', function() {
            var graph = {};
            taskScheduler.graphStateChangeStream.onNext(graph);

            expect(taskScheduler.evaluateGraphStateHandler).to.have.been.calledOnce;
            expect(taskScheduler.evaluateGraphStateHandler).to.have.been.calledWith(graph);
        });

        it('should respond to external context update events', function(done) {
            var graph = {};
            taskScheduler.evaluateExternalContextHandler.resolves([]);
            taskScheduler.externalContextStream.onNext(graph);

            setImmediate(function() {
                try {
                    expect(taskScheduler.evaluateExternalContextHandler).to.have.been.calledOnce;
                    expect(taskScheduler.evaluateExternalContextHandler)
                        .to.have.been.calledWith(graph);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });

        it('should schedule a task on graph state change', function() {
            var tasks = [
                { instanceId: 'test-task-id' }
            ];
            taskScheduler.evaluateGraphStateHandler.returns(tasks);
            taskScheduler.graphStateChangeStream.onNext();

            expect(taskScheduler.scheduleTaskHandler).to.have.been.calledOnce;
            expect(taskScheduler.scheduleTaskHandler).to.have.been.calledWith(tasks);
        });

        it('should schedule a task on external context update', function(done) {
            var tasks = [
                { instanceId: 'test-task-id' }
            ];
            taskScheduler.evaluateExternalContextHandler.resolves(tasks);
            taskScheduler.externalContextStream.onNext({});

            setImmediate(function() {
                try {
                    expect(taskScheduler.scheduleTaskHandler).to.have.been.calledOnce;
                    expect(taskScheduler.scheduleTaskHandler).to.have.been.calledWith(tasks);
                    done();
                } catch (e) {
                    done(e);
                }
            });
        });
    });

    describe('Graph Running', function() {
        before('before Graph Running', function() {
            taskScheduler = TaskScheduler.create();
        });

        it('should topologically sort linear tasks', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    '2': { waitingOn: { '1': 'finished' } },
                    '3': { waitingOn: { '2': 'finished' } }
                }
            };

            var output = taskScheduler.topologicalSortTasks(graphOne, context);
            expect(output).to.deep.equal([['1'], ['2'], ['3']]);
        });

        it('should topologically sort parallel tasks', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    '2': { },
                    '3': { waitingOn: { '2': 'finished' } }
                }
            };

            var output = taskScheduler.topologicalSortTasks(graphOne, context);
            expect(output).to.deep.equal([['1', '2'], ['3']]);
        });

        it('should topologically sort tasks with multiple dependencies', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    'A': { },
                    '2': { waitingOn: { '1': 'finished' } },
                    '3': { waitingOn: {
                            '2': 'finished',
                            'A': 'finished'
                        }
                    }
                }
            };

            var output = taskScheduler.topologicalSortTasks(graphOne, context);
            expect(output).to.deep.equal([['1', 'A'], ['2'], ['3']]);
        });

        it('should topologically sort a graph with a mix of task states', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { state: Constants.TaskStates.Succeeded },
                    '2': { state: Constants.TaskStates.Failed },
                    '3': { waitingOn: { '1': 'finished' } },
                    '4': { waitingOn: { '2': Constants.TaskStates.Failed } }
                }
            };

            var output = taskScheduler.topologicalSortTasks(graphOne, context);
            expect(output).to.deep.equal([['3', '4']]);
        });

        it('should topologically sort a complex graph', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    '2': { waitingOn: { '1': 'finished' } },
                    '3': { waitingOn: { '2': 'finished' } },
                    '4': { waitingOn: { '2': 'finished' } },
                    '5': { waitingOn: {
                            '3': 'finished',
                            '6': 'finished'
                        }
                    },
                    '6': { waitingOn: { '4': 'finished' } },
                    '7': { },
                    '8': { waitingOn: {
                            '4': 'finished',
                            '7': 'finished'
                        }
                    },
                }
            };

            var output = taskScheduler.topologicalSortTasks(graphOne, context);
            expect(output).to.deep.equal([
                ['1', '7'],
                ['2'],
                ['3', '4'],
                ['6', '8'],
                ['5']
            ]);
        });

        it('should throw on a cyclic task graph', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    '2': { injectableName: 'test2',
                            waitingOn: { '3': 'finished' }
                    },
                    '3': {
                            injectableName: 'test3',
                            waitingOn: { '2': 'finished' }
                    }
                }
            };

            expect(taskScheduler.topologicalSortTasks.bind(taskScheduler, graphOne, context))
                .to.throw(/Detected a cyclic graph with tasks test2 and test3/);
        });

        it('should throw on a cyclic task graph with > 1 levels of indirection', function() {
            var context = {};
            var graphOne = {
                tasks: {
                    '1': { },
                    '2': { injectableName: 'test2',
                            waitingOn: { 'A': 'finished' }
                    },
                    '3': { injectableName: 'test3',
                            waitingOn: { '2': 'finished' }
                    },
                    '4': { waitingOn: { '3': 'finished' } },
                    '5': { waitingOn: { '4': 'finished' } },
                    'A': { waitingOn: { '5': 'finished' } }
                }
            };

            expect(taskScheduler.topologicalSortTasks.bind(taskScheduler, graphOne, context))
                .to.throw(/Detected a cyclic graph with tasks test2 and test3/);
        });
    });
});
