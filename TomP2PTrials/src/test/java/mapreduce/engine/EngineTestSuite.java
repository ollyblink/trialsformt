package mapreduce.engine;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.broadcasting.JobBroadcastHandlersTestSuite;
import mapreduce.engine.executors.JobExecutorsTestSuite;
import mapreduce.engine.messageconsumers.JobMessageConsumersTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	JobBroadcastHandlersTestSuite.class, 
	JobExecutorsTestSuite.class,
	JobMessageConsumersTestSuite.class
})
public class EngineTestSuite {

}
