package mapreduce.engine.broadcasting;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;

import mapreduce.engine.broadcasting.messages.BCMessageStatus;
import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.engine.broadcasting.messages.IBCMessage;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;

public class CompletedTaskBCMessageTest {

	@Test
	public void test() throws InterruptedException {
		PriorityBlockingQueue<IBCMessage> bcMessages = new PriorityBlockingQueue<>();
		bcMessages.add(CompletedBCMessage
				.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, JobProcedureDomain.create(null, null, null, 0)), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(null, null, null, 0), null));
		bcMessages.add(CompletedBCMessage
				.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, JobProcedureDomain.create(null, null, null, 1)), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(null, null, null, 1), null));
		bcMessages.add(CompletedBCMessage
				.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, JobProcedureDomain.create(null, null, null, 2)), null));
		bcMessages.add(CompletedBCMessage
				.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, JobProcedureDomain.create(null, null, null, 0)), null));
		bcMessages.add(CompletedBCMessage
				.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, JobProcedureDomain.create(null, null, null, 1)), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(null, null, null, 0), null));
		int counter = 0;
		while (!bcMessages.isEmpty()) {
			IBCMessage take = bcMessages.take();
			BCMessageStatus status = null;
			if (counter == 0 || counter == 2 || counter == 3 || counter == 6 || counter == 7) {
				status = BCMessageStatus.COMPLETED_TASK;
			} else if (counter == 1 || counter == 4 || counter == 5) {
				status = BCMessageStatus.COMPLETED_PROCEDURE;
			}
			Integer procedureIndex = -1;
			if (counter >= 0 && counter <= 0) {
				procedureIndex = 2;
			} else if (counter >= 1 && counter <= 3) {
				procedureIndex = 1;
			} else if (counter >= 4 && counter <= 7) {
				procedureIndex = 0;
			}
			IDomain outputDomain = take.outputDomain();
			JobProcedureDomain d = (outputDomain instanceof JobProcedureDomain ? (JobProcedureDomain) outputDomain
					: ((ExecutorTaskDomain) outputDomain).jobProcedureDomain());
			assertEquals(procedureIndex, d.procedureIndex());
			assertEquals(status, take.status());
			counter++;
		}
	}

}
