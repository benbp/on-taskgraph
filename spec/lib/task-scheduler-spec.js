// Copyright 2015, EMC, Inc.

'use strict';

describe('Task Scheduler', function() {
    var TaskScheduler;
    var TaskGraph;
    var taskScheduler;
    var store;
    var Constants;
    var Promise;
    var Rx;

    var asyncAssertWrapper = function(done, cb) {
        return function(data) {
            try {
                cb(data);
                done();
            } catch (e) {
                done(e);
            }
        };
    };

    var setImmediateAssertWrapper = function(done, cb) {
        setImmediate(asyncAssertWrapper(done, cb));
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
        TaskGraph = helper.injector.get('TaskGraph.TaskGraph');
        store = helper.injector.get('TaskGraph.Store');
        Rx = helper.injector.get('Rx');
        Promise = helper.injector.get('Promise');
        this.sandbox = sinon.sandbox.create();
    });

    beforeEach(function() {
        this.sandbox.stub(taskScheduler, 'handleStreamError');
        this.sandbox.stub(taskScheduler, 'handleStreamSuccess');
    });

    afterEach(function() {
        this.sandbox.restore();
    });

    describe('Scheduling pipeline handler', function() {
        before(function() {
            taskScheduler = TaskScheduler.create();
        });

        describe('stream initialization and stopping', function() {
            beforeEach(function() {
                taskScheduler.pipelines = [];
            });

            it('should create disposable streams', function() {
                taskScheduler.initializePipeline().forEach(function(stream) {
                    expect(stream).to.have.property('dispose').that.is.a('function');
                });
            });

            it('should dispose streams on stop', function() {
                taskScheduler.pipelines = [
                    { dispose: sinon.stub() },
                    { dispose: sinon.stub() },
                    { dispose: sinon.stub() }
                ];
                taskScheduler.stop();
                taskScheduler.pipelines.forEach(function(mock) {
                    expect(mock.dispose).to.have.been.calledOnce;
                });
            });
        });

        it('stream success handler should return an observable', function() {
            taskScheduler.handleStreamSuccess.restore();
            expect(taskScheduler.handleStreamSuccess()).to.be.an.instanceof(Rx.Observable);
        });

        it('stream error handler should return an empty observable', function() {
            taskScheduler.handleStreamError.restore();
            expect(taskScheduler.handleStreamError('test', {})).to.be.an.instanceof(Rx.Observable);
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
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should check if task state is handled', function(done) {
                var out = {};
                taskScheduler.checkTaskStateHandled.resolves(out);
                taskHandlerStream = taskScheduler.createTaskHandlerStream(evaluateTaskStream);

                subscription = taskHandlerStream.subscribe(
                    asyncAssertWrapper(done, function(data) {
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

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error evaluating task state',
                        testError
                    );
                });
            });

            it('should not multicast stream errors', function(done) {
                taskScheduler.checkTaskStateHandled.rejects(new Error('test'));
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                taskHandlerStream = taskScheduler.createTaskHandlerStream(evaluateTaskStream);
                var subscriberStub = sinon.stub();

                subscription = taskHandlerStream.subscribe(subscriberStub);
                evaluateTaskStream.onNext();

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(subscriberStub).to.not.have.been.called;
                });
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
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should find ready tasks for graph', function(done) {
                var out = {};
                store.findReadyTasksForGraph.resolves(out);
                readyTaskStream = taskScheduler.createReadyTaskStream(evaluateGraphStream);

                subscription = readyTaskStream.subscribe(
                    asyncAssertWrapper(done, function(data) {
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

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error finding ready tasks',
                        testError
                    );
                });
            });

            it('should not multicast stream errors', function(done) {
                store.findReadyTasksForGraph.rejects(new Error('test'));
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                readyTaskStream = taskScheduler.createReadyTaskStream(evaluateGraphStream);
                var subscriberStub = sinon.stub();

                subscription = readyTaskStream.subscribe(subscriberStub);
                evaluateGraphStream.onNext();

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(subscriberStub).to.not.have.been.called;
                });
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
                taskScheduler.handleStreamError.returns(Rx.Observable.empty());
                subscription = taskScheduler.createTasksToScheduleSubscription(readyTaskStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should filter if no tasks are found', function(done) {
                store.checkoutTaskForScheduler.resolves({});

                readyTaskStream.onNext({ tasks: [] });
                readyTaskStream.onNext({ tasks: [] });
                readyTaskStream.onNext({ tasks: [] });

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkoutTaskForScheduler).to.not.have.been.called;
                });
            });

            it('should filter if a task was not checked out', function(done) {
                store.checkoutTaskForScheduler.resolves(null);

                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });
                readyTaskStream.onNext({ tasks: [ {}, {}, {} ] });

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkoutTaskForScheduler.callCount).to.equal(9);
                    expect(taskScheduler.scheduleTaskHandler).to.not.have.been.called;
                });
            });

            it('should schedule ready tasks for a graph', function(done) {
                var out = { instanceId: 'testid' };
                taskScheduler.scheduleTaskHandler.resolves();
                store.checkoutTaskForScheduler.returns(Rx.Observable.repeat(out, 3));

                readyTaskStream.onNext({ tasks: [ {} ] });

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkoutTaskForScheduler).to.have.been.called;
                    expect(taskScheduler.scheduleTaskHandler.callCount).to.equal(3);
                    expect(taskScheduler.scheduleTaskHandler).to.have.been.calledWith(out);
                });
            });

            it('should handle stream successes', function(done) {
                var out = { instanceId: 'testid' };
                store.checkoutTaskForScheduler.resolves(out);
                taskScheduler.scheduleTaskHandler.resolves(out);
                readyTaskStream.onNext({ tasks: [ {} ] });

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamSuccess).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamSuccess)
                        .to.have.been.calledWith('Task scheduled', out);
                });
            });

            it('should handle stream errors', function(done) {
                var testError = new Error('test');
                store.checkoutTaskForScheduler.rejects(testError);

                readyTaskStream.onNext({ tasks: [ {} ] });

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error scheduling task',
                        testError
                    );
                });
            });
        });

        describe('createGraphFailSubscription', function() {
            var taskHandlerStream;
            var subscription;

            before(function() {
                taskHandlerStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(taskScheduler, 'failGraph');
                subscription = taskScheduler.createGraphFailSubscription(taskHandlerStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should not fail a graph on a handled task failure', function(done) {
                var data = {
                    graphId: 'testid',
                    unhandledFailure: false
                };
                taskScheduler.failGraph.resolves(data);
                taskHandlerStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.failGraph).to.not.have.been.called;
                });
            });

            it('should fail a graph on an unhandled task failure', function(done) {
                var data = {
                    graphId: 'testid',
                    unhandledFailure: true
                };
                taskScheduler.failGraph.resolves(data);
                taskHandlerStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.failGraph).to.have.been.calledOnce;
                    expect(taskScheduler.failGraph).to.have.been.calledWith(data);
                    expect(taskScheduler.handleStreamSuccess).to.have.been.calledWith(
                        'Graph failed due to unhandled task failure',
                        data
                    );
                });
            });

            it('should handle errors related to failing a graph', function(done) {
                var testError = new Error('test fail graph error');
                taskScheduler.failGraph.rejects(testError);
                taskHandlerStream.onNext({ unhandledFailure: true });

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error failing graph',
                        testError
                    );
                });
            });
        });

        describe('createUpdateTaskDependenciesSubscription', function() {
            var taskHandlerStream;
            var subscription;

            before(function() {
                taskHandlerStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(store, 'updateDependentTasks').resolves();
                this.sandbox.stub(store, 'updateUnreachableTasks').resolves();
                this.sandbox.stub(store, 'markTaskEvaluated');
                this.sandbox.stub(taskScheduler.evaluateGraphStream, 'onNext');
                subscription = taskScheduler
                                    .createUpdateTaskDependenciesSubscription(taskHandlerStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should not take action unhandled task failures', function(done) {
                taskHandlerStream.onNext({
                    unhandledFailure: true
                });

                setImmediateAssertWrapper(done, function() {
                    expect(store.updateDependentTasks).to.not.have.been.called;
                    expect(store.updateUnreachableTasks).to.not.have.been.called;
                    expect(store.markTaskEvaluated).to.not.have.been.called;
                    expect(taskScheduler.handleStreamSuccess).to.not.have.been.called;
                    expect(taskScheduler.handleStreamError).to.not.have.been.called;
                });
            });

            it('should mark tasks as evaluated after their dependent tasks have been updated',
                    function(done) {
                var data = {
                    unhandledFailure: false
                };
                store.markTaskEvaluated.resolves(data);
                taskHandlerStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(store.markTaskEvaluated).to.have.been.calledOnce;
                    expect(store.markTaskEvaluated).to.have.been.calledWith(data);
                });
            });

            it('should update dependent and unreachable tasks on handled task failures',
                    function(done) {
                var data = {
                    unhandledFailure: false
                };
                store.markTaskEvaluated.resolves(data);
                taskHandlerStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.evaluateGraphStream.onNext).to.have.been.calledOnce;
                    expect(taskScheduler.evaluateGraphStream.onNext)
                        .to.have.been.calledWith(data);
                });
            });

            it('should handle errors related to updating task dependencies', function(done) {
                var testError = new Error('test update dependencies error');
                store.updateDependentTasks.rejects(testError);
                taskHandlerStream.onNext({ unhandledFailure: false });

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error updating task dependencies',
                        testError
                    );
                });
            });

            it('should handle errors related to marking a task as evaluated', function(done) {
                var testError = new Error('test mark task evaluated error');
                store.markTaskEvaluated.rejects(testError);
                taskHandlerStream.onNext({ unhandledFailure: false });

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error updating task dependencies',
                        testError
                    );
                });
            });
        });

        describe('createStartTaskGraphSubscription', function() {
            var startGraphStream;
            var subscription;

            before(function() {
                startGraphStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(TaskGraph, 'create').resolves();
                this.sandbox.stub(taskScheduler, 'persistInitialGraphAndTaskState').resolves();
                this.sandbox.stub(taskScheduler.evaluateGraphStream, 'onNext');
                subscription = taskScheduler.createStartTaskGraphSubscription(startGraphStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should create and persist a graph', function(done) {
                var data = {};
                TaskGraph.create.resolves(data);
                taskScheduler.persistInitialGraphAndTaskState.resolves(data);
                startGraphStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(TaskGraph.create).to.have.been.calledOnce;
                    expect(TaskGraph.create).to.have.been.calledWith(data);
                    expect(taskScheduler.persistInitialGraphAndTaskState).to.have.been.calledOnce;
                    expect(taskScheduler.persistInitialGraphAndTaskState)
                        .to.have.been.calledWith(data);
                    expect(taskScheduler.evaluateGraphStream.onNext).to.have.been.calledOnce;
                    expect(taskScheduler.evaluateGraphStream.onNext).to.have.been.calledWith(data);
                });
            });

            it('should handle errors related to starting graphs', function(done) {
                var testError = new Error('test start graph error');
                TaskGraph.create.rejects(testError);
                startGraphStream.onNext();

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error starting task graph',
                        testError
                    );
                });
            });
        });

        describe('createGraphDoneSubscription', function() {
            var readyTaskStream;
            var subscription;

            before(function() {
                readyTaskStream = new Rx.Subject();
            });

            beforeEach(function() {
                this.sandbox.stub(store, 'checkGraphDone').resolves();
                this.sandbox.stub(taskScheduler, 'publishGraphDone').resolves();
                subscription = taskScheduler.createGraphDoneSubscription(readyTaskStream);
            });

            afterEach(function() {
                subscription.dispose();
            });

            it('should filter if tasks is not empty', function(done) {
                var data = {
                    tasks: [{}, {}]
                };
                readyTaskStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkGraphDone).to.not.have.been.called;
                    expect(taskScheduler.publishGraphDone).to.not.have.been.called;
                    expect(taskScheduler.handleStreamSuccess).to.not.have.been.called;
                });
            });

            it('should filter if the graph is not done', function(done) {
                var data = {
                    tasks: []
                };
                store.checkGraphDone.resolves({ done: false });
                readyTaskStream.onNext(data);

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkGraphDone).to.have.been.calledOnce;
                    expect(taskScheduler.publishGraphDone).to.not.have.been.called;
                    expect(taskScheduler.handleStreamSuccess).to.not.have.been.called;
                });
            });

            it('should publish if a graph is done', function(done) {
                var data1 = {};
                var data2 = { done: true };
                store.checkGraphDone.resolves(data2);
                taskScheduler.publishGraphDone.resolves(data2);
                readyTaskStream.onNext(data1);

                setImmediateAssertWrapper(done, function() {
                    expect(store.checkGraphDone).to.have.been.calledOnce;
                    expect(store.checkGraphDone).to.have.been.calledWith(data1);
                    expect(taskScheduler.publishGraphDone).to.have.been.calledOnce;
                    expect(taskScheduler.publishGraphDone).to.have.been.calledWith(data2);
                    expect(taskScheduler.handleStreamSuccess).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamSuccess).to.have.been.calledWith(null, data2);
                });
            });

            it('should handle stream errors', function(done) {
                var testError = new Error('test check graph done error');
                store.checkGraphDone.rejects(testError);
                readyTaskStream.onNext();

                setImmediateAssertWrapper(done, function() {
                    expect(taskScheduler.handleStreamError).to.have.been.calledOnce;
                    expect(taskScheduler.handleStreamError).to.have.been.calledWith(
                        'Error checking graph done',
                        testError
                    );
                });
            });
        });
    });
});
