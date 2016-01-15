package mapreduce.engine.broadcasting;

import static org.junit.Assert.*;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;

import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ListMultimap;

import mapreduce.engine.executor.MRJobSubmissionManager;
import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.job.PriorityLevel;
import mapreduce.execution.procedures.StartProcedure;
import mapreduce.execution.procedures.WordCountMapper;
import mapreduce.execution.procedures.WordCountReducer;
import mapreduce.execution.task.taskdatacomposing.MaxFileSizeTaskDataComposer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.testutils.TestUtils;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.FileSize;
import mapreduce.utils.FileUtils;
import mapreduce.utils.MaxFileSizeFileSplitter;
import net.tomp2p.message.TomP2PCumulationTCP;

public class MRBroadcastHandlerTest {
	private static Random random = new Random();
	private static MRBroadcastHandler broadcastHandler;
	private static IDHTConnectionProvider dhtConnectionProvider;
	private static Job job;
	private static MRJobExecutionManagerMessageConsumer messageConsumer;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

		messageConsumer = MRJobExecutionManagerMessageConsumer.create();
		dhtConnectionProvider = TestUtils.getTestConnectionProvider(random.nextInt(50000) + 4000, 1, messageConsumer);
		messageConsumer.dhtConnectionProvider(dhtConnectionProvider);
		job = Job.create("Submitter").addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);
		dhtConnectionProvider.put(DomainProvider.JOB, job, job.id()).awaitUninterruptibly();
		broadcastHandler = dhtConnectionProvider.broadcastHandler();
	}

	@Test
	public void test() throws Exception {
		IBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(JobProcedureDomain.create(job.id(), "Submitter", "INITIAL", -1),
				JobProcedureDomain.create(job.id(), "Submitter", StartProcedure.class.getSimpleName(), 0));

		assertEquals(true, broadcastHandler.jobFutures().isEmpty());
		broadcastHandler.addExternallyReceivedMessage(msg);
		assertEquals(false, broadcastHandler.jobFutures().isEmpty());
		assertEquals(true, broadcastHandler.jobFutures().keySet().contains(job));
		ListMultimap<Job, Future<?>> jobFutures = broadcastHandler.jobFutures();
		for (Future<?> f : jobFutures.values()) {
			assertTrue(f.isDone());
			System.err.println(f.get());
		}
	}

	@Test
	public void execute() {
		MRJobSubmissionManager submitter = MRJobSubmissionManager.create(dhtConnectionProvider);
		dhtConnectionProvider.broadcastHandler().messageConsumer(messageConsumer);
		String fileInputFolderPath = System.getProperty("user.dir") + "/src/test/java/mapreduce/engine/testFiles";
		Job job = Job.create(submitter.id(), PriorityLevel.MODERATE).maxFileSize(FileSize.THIRTY_TWO_BYTES).fileInputFolderPath(fileInputFolderPath)

				.addSucceedingProcedure(WordCountMapper.create(), WordCountReducer.create(), 1, 1, false, false)
				.addSucceedingProcedure(WordCountReducer.create(), null, 1, 1, false, false);

		submitter.submit(job);
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
			//531==371
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
