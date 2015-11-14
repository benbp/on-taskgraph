// Copyright 2015, EMC, Inc.

'use strict';

describe('Task Scheduler', function() {
    var TaskScheduler;
    var taskScheduler;
    var store;
    var Constants;
    var Rx;

    var asyncAssertWrappper = function(done, cb) {
        return function(data) {
            try {
                cb(data);
                done();
            } catch (e) {
                done(e);
            }
        };
    };

    before(function() {
        var tasks = require('on-tasks');

        helper.setupInjector([
            tasks.injectables,
            require('../../lib/task-scheduler'),
            require('../../lib/task-graph'),
            require('../../lib/store'),
            require('../../lib/stores/mongo')
        ]);
        Constants = helper.injector.get('Constants');
        TaskScheduler = helper.injector.get('TaskGraph.TaskScheduler');
        store = helper.injector.get('TaskGraph.Store');
        Rx = helper.injector.get('Rx');
        this.sandbox = sinon.sandbox.create();
    });

    afterEach(function() {
        this.sandbox.restore();
    });

    describe('Scheduling pipeline handler', function() {
        var taskScheduler;

        before(function() {
            taskScheduler = TaskScheduler.create();
        });

        describe('createTaskHandlerStream', function() {
            var evaluateTaskStream;
            var taskHandlerStream;
            var subscription;

            before(function() {
                evaluateTaskStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(taskScheduler, 'checkTaskStateHandled');
                this.sandbox.stub(taskScheduler, 'handleStreamError');
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should check if task state is handled', function(done) {
                var out = {};
                taskScheduler.checkTaskStateHandled.resolves(out);
                taskHandlerStream = taskScheduler.createTaskHandlerStream(evaluateTaskStream);

                subscription = taskHandlerStream.subscribe(
                    asyncAssertWrappper(done, function(data) {
                        expect(data).to.equal(out);
                    })
                );

                evaluateTaskStream.onNext();
            });

            it('should handle stream errors', function(done) {
                var testError = new Error('test');
                taskScheduler.checkTaskStateHandled.rejects(testError);
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                taskHandlerStream = taskScheduler.createTaskHandlerStream(evaluateTaskStream);

                subscription = taskHandlerStream.subscribe(function() {});
                evaluateTaskStream.onNext();

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                            'Error evaluating task state',
                            testError
                        );
                    })
                );
            });

            it('should not multicast stream errors', function(done) {
                taskScheduler.checkTaskStateHandled.rejects(new Error('test'));
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                taskHandlerStream = taskScheduler.createTaskHandlerStream(evaluateTaskStream);
                var subscriberStub = sinon.stub();

                subscription = taskHandlerStream.subscribe(subscriberStub);
                evaluateTaskStream.onNext();

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                        expect(subscriberStub).to.not.have.been.called;
                    })
                );
            });
        });

        describe('createReadyTaskStream', function() {
            var evaluateGraphStream;
            var readyTaskStream;
            var subscription;

            before(function() {
                evaluateGraphStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(store, 'findReadyTasksForGraph');
                this.sandbox.stub(taskScheduler, 'handleStreamError');
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should find ready tasks for graph', function(done) {
                var out = {};
                store.findReadyTasksForGraph.resolves(out);
                readyTaskStream = taskScheduler.createReadyTaskStream(evaluateGraphStream);

                subscription = readyTaskStream.subscribe(
                    asyncAssertWrappper(done, function(data) {
                        expect(data).to.equal(out);
                    })
                );

                evaluateGraphStream.onNext();
            });

            it('should handle stream errors', function(done) {
                var testError = new Error('test');
                store.findReadyTasksForGraph.rejects(testError);
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                readyTaskStream = taskScheduler.createReadyTaskStream(evaluateGraphStream);

                subscription = readyTaskStream.subscribe(function() {});
                evaluateGraphStream.onNext();

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                            'Error finding ready tasks',
                            testError
                        );
                    })
                );
            });

            it('should not multicast stream errors', function(done) {
                store.findReadyTasksForGraph.rejects(new Error('test'));
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                readyTaskStream = taskScheduler.createReadyTaskStream(evaluateGraphStream);
                var subscriberStub = sinon.stub();

                subscription = readyTaskStream.subscribe(subscriberStub);
                evaluateGraphStream.onNext();

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                        expect(subscriberStub).to.not.have.been.called;
                    })
                );
            });
        });

        describe('createTasksToScheduleSubscription', function() {
            var readyTaskStream;
            var subscription;

            before(function() {
                readyTaskStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(store, 'checkoutTaskForScheduler');
                this.sandbox.stub(taskScheduler, 'scheduleTaskHandler');
                this.sandbox.stub(taskScheduler, 'handleStreamError')
                    .returns(Rx.Observable.empty());
                subscription = taskScheduler.createTasksToScheduleSubscription(readyTaskStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should find ready tasks for graph', function(done) {
                var out = {};
                store.checkoutTaskForScheduler.resolves(out);

                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(store.checkoutTaskForScheduler.callCount).to.equal(9);
                    })
                );
            });

            it('should filter if no tasks are found', function(done) {
                store.checkoutTaskForScheduler.resolves({});

                readyTaskStream.onNext({ tasks: [] });
                readyTaskStream.onNext({ tasks: [] });
                readyTaskStream.onNext({ tasks: [] });

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(store.checkoutTaskForScheduler).to.not.have.been.called;
                    })
                );
            });

            it('should filter if a task was not checked out', function(done) {
                store.checkoutTaskForScheduler.resolves(null);

                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(store.checkoutTaskForScheduler.callCount).to.equal(9);
                        expect(taskScheduler.scheduleTaskHandler).to.not.have.been.called;
                    })
                );
            });

            it('should handle stream errors', function(done) {
                var testError = new Error('test');
                store.checkoutTaskForScheduler.rejects(testError);

                readyTaskStream.onNext({ tasks: [ {} ] });

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                            'Error scheduling task',
                            testError
                        );
                    })
                );
            });

            it('should handle subscription callback errors', function(done) {
                var testError = new Error('test schedule task error');
                store.checkoutTaskForScheduler.resolves({ test: 'test' });
                taskScheduler.scheduleTaskHandler.rejects(testError);
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());

                readyTaskStream.onNext();

                process.nextTick(
                    asyncAssertWrappper(done, function() {
                        expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                            'Error scheduling task',
                            testError
                        );
                    })
                );
            });
        });
    });

    describe('Scheduling Pipeline', function() {
        beforeEach('beforeEach Scheduling Pipeline', function() {
            taskScheduler = TaskScheduler.create();
            taskScheduler.evaluateGraphStateHandler = sinon.stub();
            taskScheduler.evaluateExternalContextHandler = sinon.stub();
            taskScheduler.scheduleTaskHandler = sinon.stub();
            return taskScheduler.initializePipeline();
        });

        afterEach('afterEach Scheduling Pipeline', function() {
            return taskScheduler.stop();
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
});
