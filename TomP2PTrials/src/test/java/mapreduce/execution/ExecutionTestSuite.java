package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.computation.ProcedureTaskTupelTest;
import mapreduce.execution.datasplitting.DataSplittingTestSuite;
import mapreduce.execution.jobtask.JobTaskTestSuite;
import mapreduce.execution.scheduling.SchedulingTestSuite;
import mapreduce.execution.taskresultcomparison.TestResultComparisonTestSuite;
import mapreduce.manager.broadcasthandler.messageconsumer.MessageConsumerTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	DataSplittingTestSuite.class, 
	JobTaskTestSuite.class, 
	SchedulingTestSuite.class, 
	ProcedureTaskTupelTest.class,
	TestResultComparisonTestSuite.class
})

public class ExecutionTestSuite {

}
