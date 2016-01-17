package mapreduce.engine.messageconsumers;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	
	JobCalculationMessageConsumerTest.class,
	JobSubmissionMessageConsumerTest.class
})

public class JobMessageConsumersTestSuite {

}
