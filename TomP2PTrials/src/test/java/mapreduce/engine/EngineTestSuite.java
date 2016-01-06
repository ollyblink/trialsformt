package mapreduce.engine;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.executor.MRJobExecutionManagerTest;
import mapreduce.engine.executor.MRJobSubmissionManagerTest;
import mapreduce.engine.messageconsumer.MessageConsumerTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	MessageConsumerTestSuite.class,
	MRJobExecutionManagerTest.class,
	MRJobSubmissionManagerTest.class})
public class EngineTestSuite {

}
