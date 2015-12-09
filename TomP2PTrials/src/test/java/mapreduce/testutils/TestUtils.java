package mapreduce.testutils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.standardprocedures.NullMapReduceProcedure;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.execution.task.TaskResult;
import mapreduce.execution.task.tasksplitting.ITaskSplitter;
import mapreduce.execution.task.tasksplitting.MaxFileSizeTaskSplitter;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.utils.FileSizes;
import mapreduce.utils.FileUtils;
import mapreduce.utils.Tuple;
import net.tomp2p.peers.Number160;
import net.tomp2p.peers.PeerAddress;

public class TestUtils {
	public static Job testJobWO(IMapReduceProcedure procedure) {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}
		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newInstance("ME").nextProcedure(procedure).inputPath(inputPath).maxFileSize(FileSizes.SIXTY_FOUR_KILO_BYTE.value())
				.maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);

		MaxFileSizeTaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);
		return job;
	}

	public static Job testJob() {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		List<PeerAddress> pAds = new ArrayList<PeerAddress>();
		for (int i = 1; i <= 5; ++i) {
			pAds.add(new PeerAddress(new Number160(i)));
		}
		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newInstance("ME").nextProcedure(NullMapReduceProcedure.newInstance()).inputPath(inputPath)
				.maxFileSize(FileSizes.EIGHT_KILO_BYTE.value()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);
		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);

		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			int i = 0;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			i = 1;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			i = 2;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			i = 3;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			i = 4;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
		}
		return job;
	}

	public static Job testJob2() {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		List<PeerAddress> pAds = new ArrayList<PeerAddress>();
		for (int i = 1; i <= 5; ++i) {
			pAds.add(new PeerAddress(new Number160(i)));
		}
		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newInstance("ME").nextProcedure(NullMapReduceProcedure.newInstance()).inputPath(inputPath)
				.maxFileSize(2 * FileSizes.EIGHT_KILO_BYTE.value()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);
		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);

		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			int i = 0;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			i = 1;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
		}
		return job;
	}

	public static Job testJob3() {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		List<PeerAddress> pAds = new ArrayList<PeerAddress>();
		for (int i = 1; i <= 5; ++i) {
			pAds.add(new PeerAddress(new Number160(i)));
		}
		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newInstance("ME").nextProcedure(NullMapReduceProcedure.newInstance()).inputPath(inputPath)
				.maxFileSize(2 * FileSizes.EIGHT_KILO_BYTE.value()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);
		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);

		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			int i = 0;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
		}
		return job;
	}

	public static Job testJob4() {
		String inputPath = "/home/ozihler/git/trialsformt/TomP2PTrials/src/test/java/mapreduce/execution/task/tasksplitting/testfile";
		if (new File(inputPath + "/tmp").exists()) {
			FileUtils.INSTANCE.deleteTmpFolder(new File(inputPath + "/tmp"));
		}

		List<PeerAddress> pAds = new ArrayList<PeerAddress>();
		for (int i = 1; i <= 5; ++i) {
			pAds.add(new PeerAddress(new Number160(i)));
		}
		int maxNumberOfFinishedPeers = 3;
		Job job = Job.newInstance("ME").nextProcedure(NullMapReduceProcedure.newInstance()).inputPath(inputPath)
				.maxFileSize(2 * FileSizes.EIGHT_KILO_BYTE.value()).maxNrOfFinishedWorkersPerTask(maxNumberOfFinishedPeers);
		ITaskSplitter splitter = MaxFileSizeTaskSplitter.newInstance();
		splitter.split(job);

		BlockingQueue<Task> tasks = job.tasks(job.currentProcedureIndex());
		for (Task task : tasks) {
			int i = 0;
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.EXECUTING_TASK));
			task.updateStati(TaskResult.newInstance().sender(pAds.get(i)).status(BCMessageStatus.FINISHED_TASK));
		}
		return job;
	}

	public static void main(String[] args) {
		//// TestUtils.testJob();
		// Multimap<String, Integer> toRemoveFromDHT = ArrayListMultimap.create();
		// toRemoveFromDHT.put("Hello", 1);
		// toRemoveFromDHT.put("Hello", 1);
		// toRemoveFromDHT.put("Hello", 1);
		// toRemoveFromDHT.put("World", 1);
		// toRemoveFromDHT.put("This", 1);
		// toRemoveFromDHT.put("World", 1);
		// System.err.println(toRemoveFromDHT);
		// System.err.println(toRemoveFromDHT.values().size());
		Job testJob = testJob();
		System.err.println(testJob.tasks(testJob.currentProcedureIndex()).size());
	}

}
