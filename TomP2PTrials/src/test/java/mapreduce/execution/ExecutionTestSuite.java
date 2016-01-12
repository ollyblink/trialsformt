package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.context.DHTStorageContextTest;
import mapreduce.execution.job.JobTest;
import mapreduce.execution.task.TaskTest;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposerTest;
import obsolete.MinAssignedWorkersComparatorTest;
import obsolete.MinAssignedWorkersTaskSchedulerTest;
import obsolete.taskexecutionscheduling.sortingcomparators.MinAssignedWorkerTaskExecutionSortingComparator;

@RunWith(Suite.class)
@Suite.SuiteClasses({ JobTest.class, TaskTest.class, DHTStorageContextTest.class, MaxFileSizeTaskDataComposerTest.class,
		MinAssignedWorkersTaskSchedulerTest.class, MinAssignedWorkersComparatorTest.class, })

public class ExecutionTestSuite {

}
