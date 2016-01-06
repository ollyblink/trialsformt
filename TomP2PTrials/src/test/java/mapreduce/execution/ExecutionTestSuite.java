package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.context.DHTStorageContextTest;
import mapreduce.execution.jobtask.JobTest;
import mapreduce.execution.task.TaskTest;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersComparatorTest;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.MinAssignedWorkersTaskSchedulerTest;
import mapreduce.execution.task.scheduling.taskexecutionscheduling.sortingcomparators.MinAssignedWorkerTaskExecutionSortingComparator;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ JobTest.class, TaskTest.class, DHTStorageContextTest.class, MaxFileSizeTaskDataComposerTest.class,
		MinAssignedWorkersTaskSchedulerTest.class, MinAssignedWorkersComparatorTest.class, })

public class ExecutionTestSuite {

}
