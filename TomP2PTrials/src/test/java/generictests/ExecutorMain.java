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
		/*
		 * input: 
		 * ===================================================================================================
		 * From JobCalculationComponentTest 
		 * ===================================================================================================
		 * input: 
		 * JPD[
		 * 		J(JOB[TS(1453625753719)_RND(-2408731050369712538)_LC(2)]_S(S1))_
		 * 		JS(0)_
		 * 		PE(S1)_
		 * 		P(INITIAL_PROCEDURE)_
		 * 		PI(-1)
		 * ]
		 * ===================================================================================================
		 * From SubmitterMain 
		 * ===================================================================================================
		 * 
		 * JPD[
		 * 		J(JOB[TS(1453625831706)_RND(-4414736287683849163)_LC(2)]_S(JOBSUBMISSIONEXECUTOR[TS(1453625829137)_RND(-4271347404705378971)_LC(1)]))_
		 * 		JS(0)_
		 * 		PE(JOBSUBMISSIONEXECUTOR[TS(1453625829137)_RND(-4271347404705378971)_LC(1)])_
		 * 		P(INITIAL_PROCEDURE)_
		 * 		PI(-1)
		 * ]
		 * 
		 * 
		 * output: 
		 * ===================================================================================================
		 * From JobCalculationComponentTest 
		 * ===================================================================================================
		 * C{
		 * 	JPD[
		 * 			J(JOB[TS(1453625753719)_RND(-2408731050369712538)_LC(2)]_S(S1))_
		 * 			JS(0)_
		 * 			PE(S1)_
		 * 			P(STARTPROCEDURE)_
		 * 			PI(0)
		 * 		]
		 * }:::{
		 * 	ETD[
		 * 			T(testfile1)_
		 * 			P(S1)_
		 * 			JSI(0)
		 * 		]
		 * } 
		 * ===================================================================================================
		 * From SubmitterMain 
		 * ===================================================================================================
		 * C{
		 * 	JPD[
		 * 			J(JOB[TS(1453625831706)_RND(-4414736287683849163)_LC(2)]_S(JOBSUBMISSIONEXECUTOR[TS(1453625829137)_RND(-4271347404705378971)_LC(1)]))_
		 * 			JS(0)_
		 * 			PE(JOBSUBMISSIONEXECUTOR[TS(1453625829137)_RND(-4271347404705378971)_LC(1)])_
		 * 			P(STARTPROCEDURE)_
		 * 			PI(0)
		 * 		]
		 * }:::{
		 * 	ETD[
		 * 			T(testfile.txt_0)_
		 * 			P(JOBSUBMISSIONEXECUTOR[TS(1453625829137)_RND(-4271347404705378971)_LC(1)])_
		 * 			JSI(0)
		 * 		]
		 * }
		 * 
		 */

		JobCalculationBroadcastHandler executorBCHandler = JobCalculationBroadcastHandler.create();

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
