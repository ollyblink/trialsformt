package mapreduce.engine.messageconsumer;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.broadcasting.CompletedBCMessageTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ CompletedBCMessageTest.class, MRJobExecutorMessageConsumerTest.class })
public class MessageConsumerTestSuite {

}