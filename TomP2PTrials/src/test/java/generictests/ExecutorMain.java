package generictests;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

public class ExecutorMain {
	public static void main(String[] args) throws Exception {
		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create();

		;
		JobCalculationExecutor calculationExecutor = JobCalculationExecutor.create();

		JobCalculationMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create()
				.executor(calculationExecutor);
		executorBCHandler = JobCalculationBroadcastHandler.create()
				.messageConsumer(calculationMessageConsumer);
		int bootstrapPort = 4001;
		IDHTConnectionProvider dhtCon = DHTConnectionProvider
				.create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
				// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
				;
		dhtCon.broadcastHandler(executorBCHandler);
		calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);
		while (!dhtCon.connect().peer().isShutdown()) {
			Thread.sleep(Long.MAX_VALUE);
		}
	}
}
