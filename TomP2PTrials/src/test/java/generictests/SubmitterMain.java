package generictests;

import java.util.Random;

import mapreduce.engine.broadcasting.broadcasthandlers.JobSubmissionBroadcastHandler;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;

public class SubmitterMain {
	private static Random random = new Random();

	public static void main(String[] args) throws Exception {
		String jsMapper = FileUtils.INSTANCE.readLines(System.getProperty("user.dir")
				+ "/src/main/java/mapreduce/execution/procedures/wordcountmapper.js");
		// System.out.println(jsMapper);
		String jsReducer = FileUtils.INSTANCE.readLines(System.getProperty("user.dir")
				+ "/src/main/java/mapreduce/execution/procedures/wordcountreducer.js");
		// System.out.println(jsReducer);

		int bootstrapPort = 4442;
		int other = random.nextInt(40000) + 4000;

		JobSubmissionBroadcastHandler submitterBCHandler = JobSubmissionBroadcastHandler.create();

		IDHTConnectionProvider dhtCon = DHTConnectionProvider.create("192.168.43.65", bootstrapPort, other)
				.broadcastHandler(submitterBCHandler)
//				 .storageFilePath(System.getProperty("user.dir")
//							+ "/src/main/java/mapreduce/engine/componenttests/storage/submitter/")
				;

		JobSubmissionExecutor submissionExecutor = JobSubmissionExecutor.create()
				.dhtConnectionProvider(dhtCon);

		JobSubmissionMessageConsumer submissionMessageConsumer = JobSubmissionMessageConsumer.create()
				.dhtConnectionProvider(dhtCon).executor(submissionExecutor);

		submitterBCHandler.messageConsumer(submissionMessageConsumer);

		dhtCon.connect();

		String fileInputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfiles";
		String resultOutputFolderPath = System.getProperty("user.dir")
				+ "/src/test/java/mapreduce/engine/componenttests/testfiles/testoutputfiles/";
		Job job = Job.create(submissionExecutor.id(), PriorityLevel.MODERATE).submitterTimeToLive(10000)
				.calculatorTimeToLive(5000).maxFileSize(FileSize.MEGA_BYTE)
				.fileInputFolderPath(fileInputFolderPath)
				.resultOutputFolder(resultOutputFolderPath, FileSize.MEGA_BYTE)
				.addSucceedingProcedure(jsMapper, jsReducer, 1, 1, false, false)
				.addSucceedingProcedure(jsReducer, null, 1, 1, false, false);

		submissionExecutor.submit(job);
		while (!submissionExecutor.jobIsRetrieved(job)) {
			Thread.sleep(job.submitterTimeToLive());
		}
		System.out.println("shutting down submitter");
		dhtCon.shutdown();
		// Thread.sleep(Long.MAX_VALUE);
	}
}
