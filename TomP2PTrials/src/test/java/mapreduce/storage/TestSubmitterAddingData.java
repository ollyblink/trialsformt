package mapreduce.storage;

import org.junit.Test;

import mapreduce.execution.computation.standardprocedures.WordCountMapper;
import mapreduce.execution.job.Job;
import mapreduce.manager.MRJobSubmissionManager;
import mapreduce.utils.FileSize;

public class TestSubmitterAddingData {

	@Test
	public void test() {
		String bootstrapIP = "192.168.43.234";
		int bootstrapPort = 4000;

		MRJobSubmissionManager submitter = MRJobSubmissionManager
				.newInstance(DHTConnectionProvider.newInstance(DHTUtils.newInstance(bootstrapIP, bootstrapPort)));

		// String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/firstdesignidea/execution/datasplitting/testfile";
		String fileInputFolderPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";

		int maxNumberOfFinishedPeers = 3;
		Job job = Job.create(submitter.id()).addSubsequentProcedure(WordCountMapper.newInstance()).fileInputFolderPath(fileInputFolderPath)
				.maxFileSize(FileSize.THIRTY_TWO_KILO_BYTES).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);

		submitter.submit(job);
	}

}
