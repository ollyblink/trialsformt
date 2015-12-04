package mapreduce.manager.broadcasthandler.messageconsumer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	MessageSortingTest.class, 
	MRJobExecutorMessageConsumer_MRBroadcastHandler_Interaction_Test.class,
	MRJobExecutorMessageConsumerTest.class 
	})
public class MessageConsumerTestSuite {

}