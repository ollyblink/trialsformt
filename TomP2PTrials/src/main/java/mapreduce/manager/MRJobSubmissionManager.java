package mapreduce.manager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessageconsumer.MRJobSubmitterMessageConsumer;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.FileUtils;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Tuple;

public class MRJobSubmissionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobSubmissionManager.class);
	private IDHTConnectionProvider dhtConnectionProvider;
	private MRJobSubmitterMessageConsumer messageConsumer;
	private String id;

	private MRJobSubmissionManager(IDHTConnectionProvider dhtConnectionProvider, CopyOnWriteArrayList<Job> jobs) {
		this.dhtConnectionProvider(dhtConnectionProvider);
		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
		this.messageConsumer = MRJobSubmitterMessageConsumer.newInstance(id, jobs).canTake(true);
		new Thread(messageConsumer).start();
		dhtConnectionProvider.broadcastHandler().queue(messageConsumer.queue());
	}

	public static MRJobSubmissionManager newInstance(IDHTConnectionProvider dhtConnectionProvider) {
		return new MRJobSubmissionManager(dhtConnectionProvider, new CopyOnWriteArrayList<Job>());
	}

	/**
	 * 
	 * @param job
	 * @return
	 */
	public void submit(final Job job, final boolean awaitOnAdd) {
		List<String> keysFilePaths = new ArrayList<String>();
		FileUtils.INSTANCE.getFiles(new File(job.fileInputFolderPath()), keysFilePaths);
		dhtConnectionProvider().connect();

		for (String keyfilePath : keysFilePaths) {
			File file = new File(keyfilePath);
			Task task = Task.newInstance(keyfilePath, job.id());
			dhtConnectionProvider.addTaskData(task, task.id(), file);
			dhtConnectionProvider.addProcedureTaskPeerDomain(job, task.id(), Tuple.create(dhtConnectionProvider().peerAddress(), 0));
		}
		dhtConnectionProvider.broadcastNewJob(job);
	}

	private void createFinalTaskSplits(Job job, Collection<List<String>> allNewFileLocations, Multimap<Task, Comparable> keysForEachTask) {
		IMapReduceProcedure procedure = job.procedure(job.currentProcedureIndex());

		if (procedure != null) {
			List<Task> tasksForProcedure = new ArrayList<Task>();
			for (List<String> locations : allNewFileLocations) {

				for (int i = 0; i < locations.size(); ++i) {
					Task task = Task.newInstance(job.id()).procedure(procedure).procedureIndex(job.currentProcedureIndex())
							.maxNrOfFinishedWorkers(job.maxNrOfFinishedWorkers());
					tasksForProcedure.add(task);
					keysForEachTask.put(task, locations.get(i));
				}

			}

			job.nextProcedure(procedure, tasksForProcedure);
		} else {
			logger.error("Could not put job due to no procedure specified.");
		}
	}

	public MRJobSubmissionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtConnectionProvider;
	}

	public void shutdown() {
		this.dhtConnectionProvider.shutdown();
	}

	public String id() {
		return this.id;
	}

}
