package generictests;

import mapreduce.engine.broadcasting.broadcasthandlers.JobCalculationBroadcastHandler;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.ExecutionTestSuite;
import mapreduce.execution.jobs.Job;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;

public class ExecutorMain {
	public static void main(String[] args) throws Exception {

		JobCalculationExecutor calculationExecutor = JobCalculationExecutor.create();

		JobCalculationMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create()
				.executor(calculationExecutor);
		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create()
				.messageConsumer(calculationMessageConsumer);
		int bootstrapPort = 4442;
		IDHTConnectionProvider dhtCon = DHTConnectionProvider
				.create("192.168.43.65", bootstrapPort, bootstrapPort).broadcastHandler(executorBCHandler)
//				.storageFilePath(System.getProperty("user.dir")
//
//						+ "/src/test/java/mapreduce/engine/componenttests/storage/calculator/")
				;
		dhtCon.broadcastHandler(executorBCHandler).connect();
		calculationExecutor.dhtConnectionProvider(dhtCon);
		calculationMessageConsumer.dhtConnectionProvider(dhtCon);

		while (executorBCHandler.jobFutures().isEmpty()) {
			Thread.sleep(10);
		}
		Job job = executorBCHandler.jobFutures().keySet().iterator().next();
		while (!job.isFinished()) {
			Thread.sleep(10);
		}
		System.err.println("Shutting down executor");
		dhtCon.shutdown();
	}
	/*
	 * 12:29:32.121 [NETTY-TOMP2P - worker-client/server - -1-7] ERROR io.netty.util.ResourceLeakDetector -
	 * LEAK: AlternativeCompositeByteBuf.release() was not called before it's garbage-collected. Enable
	 * advanced leak reporting to find out where the leak occurred. To enable advanced leak reporting, specify
	 * the JVM option '-Dio.netty.leakDetectionLevel=advanced' or call ResourceLeakDetector.setLevel() See
	 * http://netty.io/wiki/reference-counted-objects.html for more information.
	 * 
	 */
}
