// Copyright 2015, EMC, Inc.
/* jshint node:true */

'use strict';

describe("Task-runner", function() {
    var runner,
    taskAndGraphId,
    noopTask,
    noopDefinition;

    before(function() {
        helper.setupInjector([require('../../lib/task-runner.js')]);
        TaskRunner = helper.injector.get('TaskGraph.TaskRunner');
        this.sandbox = sinon.sandbox.create();
    });

    beforeEach(function() {
        runner = new TaskRunner();
        taskAndGraphId = {
            taskId: 'someTaskId',
            graphId: 'someGraphId'
        };

        return runner.start();
    })

    afterEach(function() {
        this.sandbox.restore();
    })

    it("should initialize its input stream on start", function() {
        expect(runner.pipeline).to.not.equal(null);
    });

    it("should instantiate a checked out task", function() {
        runner.pipeline.onNext();

    });

    it("should filter tasks that have already been checkout out", function() {
        this.sandbox.stub(waterline.graphobjects, 'checkoutTaskForRunner');
        waterline.graphobjects.checkoutTaskForRunner.onCall(0).returns({});
        waterline.graphobjects.checkoutTaskForRunner.onCall(1).returns();

        runner.pipeline.onNext(taskAndGraphId);


    });

    it("should run an instantiated task", function() {
        runner.pipeline.onNext();
    });

    it("should publish a taskfinished event to it's output stream", function() {
        runner.pipeline.onNext();
    });

    it("should dispose all stream resources on stop", function() {
        runner.stop();
        expect(runner.pipeline.dispose).to.have.been.calledOnce;
    });

});
