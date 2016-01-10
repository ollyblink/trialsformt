package mapreduce.engine.broadcasting;

import static org.junit.Assert.*;

import java.util.concurrent.PriorityBlockingQueue;

import org.junit.Test;

import mapreduce.execution.ExecutorTaskDomain;

public class CompletedTaskBCMessageTest {

	@Test
	public void test() throws InterruptedException {
		PriorityBlockingQueue<IBCMessage> bcMessages = new PriorityBlockingQueue<>();
		bcMessages.add(CompletedBCMessage.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(0), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(0), null));
		bcMessages.add(CompletedBCMessage.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(1), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(1), null));
		bcMessages.add(CompletedBCMessage.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(1), null));
		bcMessages.add(CompletedBCMessage.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(0), null));
		bcMessages.add(CompletedBCMessage.createCompletedTaskBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(2), null));
		bcMessages.add(CompletedBCMessage.createCompletedProcedureBCMessage(ExecutorTaskDomain.create(null, null, 0, null).procedureIndex(0), null));
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
//			System.err.println(take.outputDomain().procedureIndex() + "," + take.status());
			assertEquals(procedureIndex, take.outputDomain().procedureIndex());
			assertEquals(status, take.status());
			counter++;
		}
	}

}
