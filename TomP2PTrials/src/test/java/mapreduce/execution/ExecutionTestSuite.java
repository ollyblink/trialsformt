package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.computation.ProcedureTaskTupelTest;
import mapreduce.execution.jobtask.JobTaskTestSuite;
import mapreduce.execution.task.scheduling.SchedulingTestSuite;
import mapreduce.execution.task.tasksplitting.DataSplittingTestSuite;
import mapreduce.execution.taskexecutor.TaskExecutorTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ DataSplittingTestSuite.class, JobTaskTestSuite.class, SchedulingTestSuite.class, ProcedureTaskTupelTest.class,
		TaskExecutorTestSuite.class })

public class ExecutionTestSuite {

}
