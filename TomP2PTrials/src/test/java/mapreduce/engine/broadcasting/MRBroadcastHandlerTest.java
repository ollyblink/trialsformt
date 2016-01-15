package mapreduce.engine.broadcasting;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import mapreduce.engine.broadcasting.broadcasthandlers.MapReduceBroadcastHandler;
import mapreduce.engine.executors.IExecutor;
import mapreduce.engine.executors.JobCalculationExecutor;
import mapreduce.engine.executors.JobSubmissionExecutor;
import mapreduce.engine.messageconsumers.IMessageConsumer;
import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.engine.messageconsumers.JobSubmissionMessageConsumer;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.jobs.PriorityLevel;
import mapreduce.execution.tasks.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.storage.DHTConnectionProvider;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;

public class MRBroadcastHandlerTest {
	private static Random random = new Random();
	// private static MapReduceBroadcastHandler broadcastHandler;
	// private static IDHTConnectionProvider dhtConnectionProvider;
	// private static Job job;
	// private static JobCalculationMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		// messageConsumer = JobCalculationMessageConsumer.create();
		// dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, messageConsumer);
		// messageConsumer.dhtConnectionProvider(dhtConnectionProvider);
		// job = Job.create("Submitter").addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
		// .addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);
		// dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		// broadcastHandler = dhtConnectionProvider.broadcastHandler();
	}

	@Test
	public void test() throws Exception {
		// IBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(job.id(), "Submitter", "INITIAL", -1),
		// JobProcedureDomain.create(job.id(), "Submitter", StartProcedure.class.getSimpleName(), 0));
		//
		// assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		// broadcastHandler.addExternallyReceivedMessage(msg);
		// assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		// assertEquals(true, broadcastHandler.jobFutures().keySet().contains(job));
		// ListMultimap<Job, Future<?>> jobFutures = broadcastHandler.jobFutures();
		// for (Future<?> f : jobFutures.values()) {
		// assertTrue(f.isDone());
		// System.err.println(f.get());
		// }
	}

	@Test
	public void execute() throws Exception {
		String jsMapper = "function process(keyIn, valuesIn, context){" + " for each (var value in valuesIn) { "
				+ "    var splits = value.split(\" \");	" + "    for each (var split in splits){ " + "      context.write(split, 1);	" + "    } "
				+ "  }" + "}";
		String jsReducer = "function process(keyIn, valuesIn, context){" + "  var count = 0; " + "  for each (var value in valuesIn) { "
				+ "    count += value;	" + "  } " + "  context.write(keyIn, count); " + "} ";

		
		int bootstrapPort = random.nextInt(40000) + 4000;
		
		MapReduceBroadcastHandler submitterBCHandler = MapReduceBroadcastHandler.create(1);
		
		IDHTConnectionProvider dhtCon = DHTConnectionProvider.create("192.168.43.65", bootstrapPort, bootstrapPort)
										.nrOfPeers(1)
										.broadcastHandler(submitterBCHandler)
										.storageFilePath("C:\\Users\\Oliver\\Desktop\\storage");
		
		JobSubmissionExecutor submissionExecutor =  JobSubmissionExecutor.create()
											.dhtConnectionProvider(dhtCon);
			
		JobSubmissionMessageConsumer submissionMessageConsumer = JobSubmissionMessageConsumer.create()
														.dhtConnectionProvider(dhtCon)
														.executor(submissionExecutor);
		
		submitterBCHandler.messageConsumer(submissionMessageConsumer);
 
		dhtCon.connect(); 
		int other = random.nextInt(40000) + 4000;
		
		MapReduceBroadcastHandler executorBCHandler = MapReduceBroadcastHandler.create(1);
		
		IDHTConnectionProvider dhtCon2 = DHTConnectionProvider.create("192.168.43.65", bootstrapPort, other)
											.nrOfPeers(1)
											.broadcastHandler(submitterBCHandler)
											.storageFilePath("C:\\Users\\Oliver\\Desktop\\storage");
	
	 
		dhtCon2.connect(); 
		IExecutor calculationExecutor = JobCalculationExecutor.create()
														.dhtConnectionProvider(dhtCon2);
		
		IMessageConsumer calculationMessageConsumer = JobCalculationMessageConsumer.create()
														.dhtConnectionProvider(dhtCon2)
														.executor(calculationExecutor);
		
		executorBCHandler.messageConsumer(calculationMessageConsumer);

		
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";
		Job job = Job.create(submissionExecutor.id(), PriorityLevel.MODERATE)
				.maxFileSize(FileSize.THIRTY_TWO_BYTES).fileInputFolderPath(fileInputFolderPath) 
				.addSucceedingProcedure(jsMapper, jsReducer, 1, 1, false, false).addSucceedingProcedure(jsReducer, null, 1, 1, false, false);

		 submissionExecutor.submit(job);
		try {
			Thread.sleep(Long.MAX_VALUE);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";
		Job job = Job.create("S").maxFileSize(FileSize.THIRTY_TWO_BYTES).fileInputFolderPath(fileInputFolderPath);

		List<String> keysFilePaths = new ArrayList<String>();
		File file = new File(job.fileInputFolderPath());
		FileUtils.INSTANCE.getFiles(file, keysFilePaths);

		// Get the number of files to be expected
		// long overallFileSizes = 0;
		int nrOfFiles = 0;
		MaxFileSizeTaskDataComposer taskDataComposer = MaxFileSizeTaskDataComposer.create();
		taskDataComposer.splitValue("\n").maxFileSize(job.maxFileSize());
		long totCounted = 0;
		for (String fileName : keysFilePaths) {
			File file2 = new File(fileName);
			long fileSize = file2.length();
			System.out.println("File Size: " + fileSize);
			ArrayList<String> lines = FileUtils.readLinesFromFile(fileName);

			String allData = "";
			for (String line : lines) {
				line = taskDataComposer.remainingData() + "\n" + line;
				List<String> splitToSize = taskDataComposer.splitToSize(line);
				for (String split : splitToSize) {
					totCounted += split.getBytes(Charset.forName(taskDataComposer.fileEncoding())).length;
					allData += split + " ";
				}
			}
			// 531==371
			allData += taskDataComposer.remainingData();
			totCounted += taskDataComposer.remainingData().getBytes(Charset.forName(taskDataComposer.fileEncoding())).length;
			System.out.println("All data: " + allData);
			System.out.println("File Size from task data composer: " + totCounted);
			nrOfFiles += (int) (fileSize / job.maxFileSize().value());
			if (fileSize % job.maxFileSize().value() > 0) {
				++nrOfFiles;
			}
		}
		System.out.println("Max file size: " + job.maxFileSize().value());
		System.out.println("Nr of files according to file size: " + nrOfFiles);
		System.out.println("Nr of files according to task data composer: "
				+ ((totCounted / job.maxFileSize().value()) + ((totCounted / job.maxFileSize().value()) > 0 ? 1 : 0)));
	}
}
