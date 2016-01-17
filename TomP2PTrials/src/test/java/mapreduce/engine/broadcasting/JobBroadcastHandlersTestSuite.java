package mapreduce.engine.broadcasting;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandlerTest;
import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandlerTest;
import mapreduce.engine.broadcasting.broadcasthandlers.timeouts.JobCalculationTimeoutTest;
import mapreduce.engine.broadcasting.broadcasthandlers.timeouts.JobSubmissionTimeoutTest;
import mapreduce.engine.broadcasting.messages.CompletedTaskBCMessageTest;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	JobCalculationTimeoutTest.class,
	JobSubmissionTimeoutTest.class,
	JobCalculationBroadcastHandlerTest.class,
	JobSubmissionBroadcastHandlerTest.class,
	CompletedTaskBCMessageTest.class
})

public class JobBroadcastHandlersTestSuite {
 

}
