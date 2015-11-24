package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.datasplitting.DataSplittingTestSuite;
import mapreduce.execution.jobtask.JobTest;
import mapreduce.execution.jobtask.TaskTest;
import mapreduce.execution.scheduling.SchedulingTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DataSplittingTestSuite.class,
	JobTest.class,
	TaskTest.class,
	SchedulingTestSuite.class
})
public class ExecutionTestSuite {
 
}
