// Copyright 2015, EMC, Inc.
/* jshint node:true */

'use strict';

describe("Task-runner", function() {
    var runner,
    Task = {},
    TaskRunner,
    waterline = { graphobjects: {} },
    taskAndGraphId,
    stubbedTask = {run:function() {}},
    taskDef = {
        friendlyName: 'testTask',
        implementsTask: 'fakeBaseTask',
        runJob: 'fakeJob',
        options: {},
        properties: {}
    };

    before(function() {
        helper.setupInjector([
                require('../../lib/task-runner.js'),
                helper.di.simpleWrapper(Task, 'Task.Task'),
                helper.di.simpleWrapper(waterline, 'Services.Waterline')
        ]);
        TaskRunner = helper.injector.get('TaskGraph.TaskRunner');
        this.sandbox = sinon.sandbox.create();
        waterline.graphobjects.checkoutTaskForRunner = function() {};
    });

    beforeEach(function() {
        runner = new TaskRunner();
        taskAndGraphId = {
            taskId: 'someTaskId',
            graphId: 'someGraphId'
        };
        stubbedTask.run = this.sandbox.stub();
        Task.create = this.sandbox.stub().returns(stubbedTask);

        return runner.start();
    });

    afterEach(function() {
        this.sandbox.restore();
    });

    it("should initialize its input stream", function() {
        expect(runner.pipeline).to.not.equal(null);
    });

    it("should instantiate a checked out task", function() {
        runner.inputStream.onNext(taskAndGraphId);

    });

    it("should filter tasks that have already been checkout out", function(done) {
        this.sandbox.stub(waterline.graphobjects, 'checkoutTaskForRunner');
        this.sandbox.stub(runner, 'handleTask');
      //  console.log('woof');
        console.log(runner.subscriptions);
        waterline.graphobjects.checkoutTaskForRunner.onCall(0).resolves(taskDef);
        waterline.graphobjects.checkoutTaskForRunner.onCall(1).resolves(undefined);

        runner.inputStream.onNext(taskAndGraphId);
        runner.inputStream.onNext(taskAndGraphId);

        setImmediate(function() {
            expect(waterline.graphobjects.checkoutTaskForRunner).to.be.calledTwice;
            expect(stubbedTask.run).to.be.calledOnce;
            done();
        });

    });

    it("should run an instantiated task", function() {
        runner.inputStream.onNext(taskAndGraphId);

    });

    it("should publish a taskfinished event to its output stream", function() {
        runner.inputStream.onNext(taskAndGraphId);
    });

    it("should dispose all stream resources on stop", function() {
        this.sandbox.spy(runner.inputStream, 'dispose');
        runner.stop();
        expect(runner.inputStream.dispose).to.have.been.calledOnce;
    });

});
