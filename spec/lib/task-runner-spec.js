// Copyright 2015, EMC, Inc.
/* jshint node:true */

'use strict';

describe("Task-runner", function() {
    var runner,
    Task = {},
    TaskRunner,
    taskMessenger = {},
    store = {},
    taskAndGraphId,
    stubbedTask = {run:function() {}},
    Rx,
    taskDef = {
        friendlyName: 'testTask',
        implementsTask: 'fakeBaseTask',
        runJob: 'fakeJob',
        options: {},
        properties: {}
    };

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
        helper.setupInjector([
                require('../../lib/task-runner.js'),
                require('../../lib/messenger.js'),
                require('../../lib/messengers/messenger-AMQP.js'),
                helper.di.simpleWrapper(taskMessenger, 'Task.Messenger.AMQP'),
                helper.di.simpleWrapper(Task, 'Task.Task'),
                helper.di.simpleWrapper(store, 'TaskGraph.Store')
        ]);
        Rx = helper.injector.get('Rx');
        TaskRunner = helper.injector.get('TaskGraph.TaskRunner');
        this.sandbox = sinon.sandbox.create();
    });

    beforeEach(function() {
        runner = TaskRunner.create();
        taskAndGraphId = {
            taskId: 'someTaskId',
            graphId: 'someGraphId'
        };
        stubbedTask.run = this.sandbox.stub();
        Task.create = this.sandbox.stub().returns(stubbedTask);
        store.checkoutTaskForRunner = this.sandbox.stub();
        store.getTaskById = this.sandbox.stub();
        store.heartbeatTasks = this.sandbox.stub();
        store.setTaskState = this.sandbox.stub();
    });

    afterEach(function() {
        this.sandbox.restore();
    });

    describe('stream setup/cleanup', function() {

        describe('start', function() {

            it('should mark itself running', function(done) {
                runner.running = false;
                runner.subscribeRunTask = this.sandbox.stub();
                runner.initializePipeline = this.sandbox.stub();
                return runner.start()
                .then(function() {
                    try {
                        expect(runner.isRunning()).to.equal(true);
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });

            it('should initialize its pipelines', function(done) {
                runner.running = false;
                runner.subscribeRunTask = this.sandbox.stub();
                runner.initializePipeline = this.sandbox.stub();
                return runner.start()
                .then(function() {
                    try {
                        expect(runner.initializePipeline).to.have.been.calledOnce;
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });

            it('should subscribe to a task messenger', function(done) {
                runner.running = false;
                runner.subscribeRunTask = this.sandbox.stub();
                runner.initializePipeline = this.sandbox.stub();
                return runner.start()
                .then(function() {
                    try {
                        expect(runner.subscribeRunTask).to.have.been.calledOnce;
                        done();
                    } catch (e) {
                        done(e);
                    }
                });
            });

            describe('initializePipeline', function() {

                it('should return disposable subscriptions', function() {
                    runner.initializePipeline().forEach(function(subscription) {
                        expect(subscription).to.have.property('dispose')
                        .that.is.a('function');
                    });
                });


                describe('createRunTaskSubscription', function() {

                    it('should not flow if task runner is not running', function(done) {
                        runner.running = false;
                        var subscription = runner.createRunTaskSubscription(runner.runTaskStream);
                        runner.runTaskStream.onNext(taskAndGraphId);
                        setImmediateAssertWrapper(done, function() {
                            expect(store.checkoutTaskForRunner).to.not.have.been.called;
                            subscription.dispose();
                        });
                    });

                    it('should return a disposable subscription', function() {
                        runner.running = true;
                        var subscription = runner.createRunTaskSubscription(runner.runTaskStream);
                        expect(subscription).to.have.property('dispose').that.is.a('function');
                    });

                    it('should filter empty tasks', function(done) {
                        runner.running = true;
                        store.checkoutTaskForRunner.resolves(undefined);
                        var subscription = runner.createRunTaskSubscription(runner.runTaskStream);
                        runner.runTaskStream.onNext(taskAndGraphId);
                        setImmediateAssertWrapper(done, function() {
                            expect(store.checkoutTaskForRunner.callCount).to.equal(1);
                            expect(store.getTaskById).to.not.have.been.called;
                            subscription.dispose();
                        });
                    });

                    it('should run a task', function(done) {
                        runner.running = true;
                        store.checkoutTaskForRunner.resolves(taskAndGraphId);
                        store.getTaskById.resolves(taskDef);
                        runner.runTask = this.sandbox.stub().resolves();
                        runner.handleStreamSuccess = this.sandbox.stub();
                        var subscription = runner.createRunTaskSubscription(runner.runTaskStream);
                        runner.runTaskStream.onNext(taskAndGraphId);
                        setImmediateAssertWrapper(done, function() {
                            expect(runner.runTask).to.have.been.calledOnce;
                            expect(runner.handleStreamSuccess).to.have.been.calledOnce;
                            subscription.dispose();
                        });
                    });

                    it('should handle any errors in the stream', function(done) {
                        runner.running = true;
                        store.checkoutTaskForRunner.resolves(taskAndGraphId);
                        store.getTaskById.resolves(taskDef);
                        runner.runTask = this.sandbox.stub().resolves();

                        var mapFuncs = [
                            store.checkoutTaskForRunner,
                            store.getTaskById,
                            runner.runTask
                        ];

                        var errSpy = sinon.spy(runner, 'handleStreamError');
                        _.forEach(mapFuncs, function(func) {
                            runner.createRunTaskSubscription(runner.runTaskStream);
                            func.throws(new Error());
                            runner.runTaskStream.onNext({});
                        });
                        setImmediateAssertWrapper(done, function() {
                            expect(errSpy.callCount).to.equal(3);
                        });
                    });
                });

                describe('createHeartbeatSubscription', function() {

                    it('should heartbeat Tasks on an interval', function(done) {
                        runner.running = true;
                        runner.heartbeatInterval = 20;
                        store.heartbeatTasks = this.sandbox.stub().resolves();
                        runner.handleStreamSuccess = this.sandbox.stub();
                        var subscription = runner.createHeartbeatSubscription();
                        setTimeout(asyncAssertWrapper(done, function() {
                            expect(store.heartbeatTasks.callCount).to.equal(2);
                            expect(runner.handleStreamSuccess.callCount).to.equal(2);
                            subscription.dispose();
                        }), 59);
                    });

                    it('should not beat when the runner is not running', function(done) {
                        runner.running = false;
                        var subscription = runner.createHeartbeatSubscription(1);
                        store.heartbeatTasks = this.sandbox.stub().resolves();
                        setImmediateAssertWrapper(done, function() {
                            expect(store.heartbeatTasks).to.not.have.been.called;
                            subscription.dispose();
                        });
                    });

                    it('should return a disposable subscription', function() {
                        var subscription = runner.createHeartbeatSubscription(500);
                        expect(subscription).to.have.property('dispose').that.is.a('function');
                        subscription.dispose();
                    });

                    it('should stop the runner on Error', function(done) {
                        runner.running = true;
                        runner.heartbeatInterval = 1;
                        store.heartbeatTasks = this.sandbox.stub().throws(new Error('test error'));
                        runner.stop = this.sandbox.stub().resolves();
                        runner.handleStreamSuccess = this.sandbox.stub();
                        var subscription = runner.createHeartbeatSubscription();
                        setTimeout(asyncAssertWrapper(done, function() {
                            expect(store.heartbeatTasks).to.have.been.calledOnce;
                            expect(runner.handleStreamSuccess).to.have.been.calledOnce;
                            expect(runner.stop).to.have.been.calledOnce;
                            subscription.dispose();
                        }), 20);
                    });
                });
            });

        });

        describe('stop', function() {
            it('should mark itself not running', function() {
                runner.running = true;
                runner.stop();
                expect(runner.isRunning()).to.equal(false);
            });

            it('should dispose all pipelines', function() {
                var mockOne = { dispose: sinon.stub() },
                mockTwo = { dispose: sinon.stub() };
                runner.pipelines = [
                    mockOne,
                    mockTwo
                ];
                runner.stop();
                expect(runner.pipelines.length).to.equal(0);
                expect(mockOne.dispose).to.have.been.calledOnce;
                expect(mockTwo.dispose).to.have.been.calledOnce;
            });
        });

    });

    describe('subscribeRunTask', function() {
        it("should wrap the taskMessenger's subscribeRunTask method", function() {
            taskMessenger.subscribeRunTask = this.sandbox.stub();
            runner.subscribeRunTask();
            expect(taskMessenger.subscribeRunTask).to.have.been.calledOnce;
        });
    });

    describe('publishTaskFinished', function() {
        it("should wrap the taskMessenger's publishTaskFinished", function() {
            taskMessenger.publishTaskFinished = this.sandbox.stub().resolves();
            var finishedTask = {
                taskId: 'aTaskId',
                context: { graphId: 'aGraphId'},
                state: 'finished'
            };
            runner.publishTaskFinished(finishedTask)
            .then(function() {
                expect(taskMessenger.publishTaskFinished).to.have.been.calledOnce;
            });
        });
    });

    describe('stream handlers', function() {
        it('stream success handler should return an observable', function() {
            expect(runner.handleStreamSuccess()).to.be.an.instanceof(Rx.Observable);
        });

        it('stream error handler should return an empty observable', function() {
            expect(runner.handleStreamError('test', {})).to.be.an.instanceof(Rx.Observable);
        });
    });

    describe('runTask', function() {
        it('should return an Observable', function() {
        });

        it('should instantiate a task', function() {
        });

        it('should run a task', function() {
        });

        it('should add and remove tasks from its activeTasks list', function() {
        });

        it('should publish a task finished event', function() {
        });

        it('should set the state of a task', function() {
        });

        it('should not crash the parent pipeline if a task fails', function(){
        });
    });
});
