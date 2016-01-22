package generictests;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;

public class ExecutorMain {
	public static void main(String[] args) throws Exception {
		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create(1);

		int bootstrapPort = 4001;
		IDHTConnectionProvider dhtCon2 = DHTConnectionProvider
				.create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
				// .storageFilePath("C:\\Users\\Oliver\\Desktop\\storage")
				;

		IExecutor calculationExecutor = JobCalculationExecutor.create().dhtConnectionProvider(dhtCon2);

		IMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create()
				.dhtConnectionProvider(dhtCon2).executor(calculationExecutor);

		executorBCHandler.messageConsumer(calculationMessageConsumer);
		while (!dhtCon2.connect().peer().isShutdown()) {
			Thread.sleep(Long.MAX_VALUE);
		}
	}
}
