package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.broadcasthandler.messageconsumer.MRJobExecutorMessageConsumer;
import mapreduce.execution.broadcasthandler.messageconsumer.MRJobExecutorMessageConsumerTest;
import mapreduce.execution.broadcasthandler.messageconsumer.MessageSortingTest;
import mapreduce.execution.computation.ProcedureTaskTupelTest;
import mapreduce.execution.datasplitting.DataSplittingTestSuite;
import mapreduce.execution.jobtask.JobTest;
import mapreduce.execution.jobtask.TaskTest;
import mapreduce.execution.scheduling.SchedulingTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DataSplittingTestSuite.class,
	JobTest.class,
	TaskTest.class,
	SchedulingTestSuite.class,
	ProcedureTaskTupelTest.class,
	MessageSortingTest.class,
	MRJobExecutorMessageConsumerTest.class
})
public class ExecutionTestSuite {
 
}
