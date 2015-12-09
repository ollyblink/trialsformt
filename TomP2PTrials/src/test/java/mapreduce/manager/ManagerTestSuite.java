package mapreduce.manager;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.manager.broadcasthandler.messageconsumer.MessageConsumerTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	MessageConsumerTestSuite.class,
	MRJobExecutionManagerTest.class,
	MRJobSubmissionManagerTest.class})
public class ManagerTestSuite {

}
