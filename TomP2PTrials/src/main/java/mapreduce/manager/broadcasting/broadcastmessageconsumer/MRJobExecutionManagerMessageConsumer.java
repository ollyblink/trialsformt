package mapreduce.manager.broadcasting.broadcastmessageconsumer;

import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.PriorityBlockingQueue;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task2;
import mapreduce.manager.MRJobExecutionManager;
import mapreduce.manager.broadcasting.broadcastmessages.IBCMessage;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	private MRJobExecutionManager jobExecutor;

	private MRJobExecutionManagerMessageConsumer(MRJobExecutionManager jobExecutor) {
		this.jobExecutor = jobExecutor;
	}

	public static MRJobExecutionManagerMessageConsumer create(MRJobExecutionManager jobExecutor) {
		return new MRJobExecutionManagerMessageConsumer(jobExecutor);
	}

	/**
	 * Use this for interrupting execution (canExecute(false))
	 * 
	 * @param mrJobExecutor
	 * @return
	 */
	public MRJobExecutionManagerMessageConsumer jobExecutor(MRJobExecutionManager mrJobExecutor) {
		this.jobExecutor = mrJobExecutor;
		return this;
	}

	@Override
	public MRJobExecutionManagerMessageConsumer canTake(boolean canTake) {
		return (MRJobExecutionManagerMessageConsumer) super.canTake(canTake);
	}

	@Override
	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain, int tasksSize) {
		Procedure currentProcedure = jobs.firstKey().currentProcedure();
		if (currentProcedure.procedureIndex() == outputDomain.jobProcedureDomain().procedureIndex()) { // Same procedure is executed
			if (currentProcedure.inputDomain().equals(inputDomain)) {
				Task2 receivedTask = Task2.create(outputDomain.taskId());
				List<Task2> tasksToUpdate = currentProcedure.tasks();
				if (tasksToUpdate.contains(receivedTask)) {
					Task2 thisTask = tasksToUpdate.get(tasksToUpdate.indexOf(receivedTask));
					if (!thisTask.isFinished()) {
						thisTask.addOutputDomain(inputDomain);
						if (thisTask.isFinished()) { // May be finished now...
							if (thisTask.isActive()) { // Currently executing... needs abortion and cleanup
								jobExecutor.abortExecution();
							}
							// Add data to procedure domain!
							if (!thisTask.isInProcedureDomain()) {

								JobProcedureDomain outputJobProcedureDomain = new JobProcedureDomain(jobs.firstKey().id(), this.jobExecutor.id(),
										currentProcedure.executable().getClass().getSimpleName(), currentProcedure.procedureIndex());
								jobExecutor.tryToAddTaskDataToProcedureDomain(thisTask, jobs.firstKey().maxNrOfDHTActions(),
										outputJobProcedureDomain);
							}
						}
					}
				} else {
					receivedTask.addOutputDomain(outputDomain);
					currentProcedure.addTask(receivedTask);
				}
				this.jobExecutor.executeJob(jobs.firstKey());
			} else { // May have to change input data location (inputDomain)
				// executor of received message executes on different input data! Need to synchronize
				// Simply compare which one was earlier
				if (currentProcedure.inputDomain().creationTime() > inputDomain.creationTime()) {
					// We were later...
					if (currentProcedure.isActive()) {
						jobExecutor.abortExecution();
					}
					currentProcedure.inputDomain(inputDomain);
					currentProcedure.addOutputDomain(outputDomain);
					this.jobExecutor.executeJob(nextJob());
				} else {// if (currentJob.currentProcedure().inputDomain().procedureCreationTime <= inputDomain.procedureCreationTime)
					// Do nothing... Continue execution
				}
			}
			if (currentProcedure.tasksSize() == 0) { // Means it was not yet set when retrieving data
				currentProcedure.tasksSize(tasksSize); // not really the place to put it but where else...
			}
		} else if (currentProcedure.procedureIndex() < outputDomain.jobProcedureDomain().procedureIndex()) {
			// Means this executor is behind in the execution than the one that sent this message
			if (currentProcedure.isActive()) {
				jobExecutor.abortExecution();
			}
			while (currentProcedure.procedureIndex() < outputDomain.jobProcedureDomain().procedureIndex()) {
				jobs.firstKey().incrementProcedureIndex();
			}
			currentProcedure.inputDomain(inputDomain);
			currentProcedure.addOutputDomain(outputDomain);
			currentProcedure.tasksSize(tasksSize);

			this.jobExecutor.executeJob(nextJob());
			if (currentProcedure.tasksSize() == 0) { // Means it was not yet set when retrieving data
				currentProcedure.tasksSize(tasksSize); // not really the place to put it but where else...
			}
		} else { // if(currentJob.currentProcedure().procedureIndex() > outputDomain.procedureIndex)
			// Means this executor is further in the execution than the one that sent this message
			// Ignore...
		}
	}

	@Override
	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain, int tasksSize) {
		Procedure currentProcedure = jobs.firstKey().currentProcedure();
		if (currentProcedure.procedureIndex() == outputDomain.procedureIndex()) { // Same procedure is executed
			if (currentProcedure.inputDomain().equals(inputDomain)) {
				currentProcedure.addOutputDomain(outputDomain);
				if (currentProcedure.isFinished()) {
					if (currentProcedure.isActive()) {
						jobExecutor.abortExecution();
					}
					jobs.firstKey().incrementProcedureIndex();

					this.jobExecutor.executeJob(nextJob());
				}
			} else { // May have to change input data location (inputDomain)
				// executor of received message executes on different input data! Need to synchronize
				// Simply compare which one was earlier
				if (currentProcedure.inputDomain().creationTime() > inputDomain.creationTime()) {
					// We were later...
					if (currentProcedure.isActive()) {
						jobExecutor.abortExecution();
					}
					currentProcedure.inputDomain(inputDomain);
					currentProcedure.addOutputDomain(outputDomain);

					this.jobExecutor.executeJob(nextJob());
				} else {// if (currentJob.currentProcedure().inputDomain().procedureCreationTime <= inputDomain.procedureCreationTime)
					// Do nothing... Continue execution
				}
			}
		} else if (currentProcedure.procedureIndex() < outputDomain.procedureIndex()) {
			// Means this executor is behind in the execution than the one that sent this message
			if (currentProcedure.isActive()) {
				this.jobExecutor.executeJob(jobs.firstKey());
			}
			while (currentProcedure.procedureIndex() < outputDomain.procedureIndex()) {
				jobs.firstKey().incrementProcedureIndex();
			}
			currentProcedure.inputDomain(inputDomain);
			currentProcedure.addOutputDomain(outputDomain);

			this.jobExecutor.executeJob(nextJob());
		} else { // if(currentJob.currentProcedure().procedureIndex() > outputDomain.procedureIndex)
			// Means this executor is further in the execution than the one that sent this message
			// Ignore...
		}
	}

	// @Override
	// public void handleReceivedJob(String receivedJobId, int currentProcedureIndex) {
	// // logger.info("Received job: " + job);
	//
	// Job jobDummyToRetrieve = Job.create(receivedJobId);
	// if (!jobs.contains(jobDummyToRetrieve)) { // Job was not yet received
	// jobExecutor.dhtConnectionProvider().get(DomainProvider.JOB, receivedJobId).addListener(new BaseFutureAdapter<FutureGet>() {
	//
	// @Override
	// public void operationComplete(FutureGet future) throws Exception {
	// if (future.isSuccess()) {
	// Job job = (Job) future.data().object();
	// // Make sure it's on the right procedure
	// logger.info("Successfully retrieved and added job");
	//
	// jobs.add(job);
	// executeNext(job, currentProcedureIndex);
	// } else {
	// logger.warn("No success on retrieving job with id: " + receivedJobId);
	// }
	// }
	//
	// });
	// // logger.info("Added new job");
	// } else { // Job was already received once. So probably the procedure changed
	// // if (!sender.equals(jobExecutor.id())) {
	// executeNext(jobs.get(jobs.indexOf(jobDummyToRetrieve)), currentProcedureIndex);
	//
	// // logger.info("job2.currentProcedureIndex() == job.currentProcedureIndex()" + job2.previousProcedure() + " == " + job.previousProcedure()
	// // + " ? " + (job2.previousProcedure().equals(job.previousProcedure())));
	// // if (job2.previousProcedure().equals(job.previousProcedure())) { // next procedure
	// // jobs.set(index, job); // replaced
	// // logger.info("replaced old job with new version as the procedure index increased: from " + job2.previousProcedure() + " to "
	// // + job.previousProcedure());
	// // }
	// // }
	// }
	//
	// }
	//
	// public void executeNext(Job job, int currentProcedureIndex) {
	// while (currentProcedureIndex > job.currentProcedure().procedureIndex()) {
	// job.incrementProcedureIndex();
	// }
	// synchronized (jobs) {
	// Collections.sort(jobs);
	// }
	// // logger.info("Jobs: " + jobs);
	// if (jobExecutor.currentlyExecutedJob() != null) {
	// if (!jobExecutor.currentlyExecutedJob().equals(jobs.get(0))) {
	//
	// // logger.info("jobExecutor.currentlyExecutedJob() != null");
	// // The job currently executing is not the one with highest priority
	// // anymore
	// // Abort the currently executing job (meaning, interrupt until later resume... This finishes the task executing atm
	// jobExecutor.isExecutionAborted(true);
	// jobExecutor.execute(jobs.get(0));
	// } else {
	// if (!jobExecutor.currentlyExecutedJob().isActive()) {
	// jobExecutor.execute(jobs.get(0));
	// }
	// }
	//
	// } else {
	// jobExecutor.execute(jobs.get(0));
	// }
	// }
	//
	// @Override
	// public void handleTaskExecutionStatusUpdate(String receivedJobID, int receivedCurrentProcedureIndex, Task receivedTask, Number160 resultHash) {
	// // Make sure the transmitted job only contains procedure information for the tasks that need to be updated...
	// // Don't forget the isFinished for all Job/Procedure/Task and isActive Job/Task
	// // public void TaskUpdate(Job receivedJob, Task receivedTask, TaskResult taskResultUpdate) {
	// // Checking the jobs
	// // Job here needs to only have tasks for the currently executed procedure used
	// Job receivedJobDummy = Job.create(receivedJobID);
	// if (jobs.contains(receivedJobDummy)) { // If it contains it, we may update it
	// int jobIndex = jobs.indexOf(receivedJobDummy);
	// Job jobToUpdate = jobs.get(jobIndex);
	//
	// if (jobToUpdate.isActive()) { // We only want to update a currently executed job
	// // Check the current procedure's actuality...
	// ProcedureInformation currentPI = jobToUpdate.currentProcedure();
	// // ProcedureInformation receivedPI = receivedJobDummy.currentProcedure();
	// // Checking the procedures' orders: are the same procedures executed?
	// if (currentPI.procedureIndex() == receivedCurrentProcedureIndex) {
	// // This is what we hope for... the received index is the same as the currently executed job... Everything fine and can start
	// // comparing the tasks...
	// List<Task> tasks = currentPI.tasks(); // Be aware, these task list is also used by the taskScheduler in jobExecutor!
	// // Checking the tasks... Either there is a new task received that we were not yet executing, or an already executed task has to be
	// // updated
	//
	// if (!tasks.contains(receivedTask)) {
	// // This executor has not yet had that task which means it has not created it yet... simply add id to the tasks and it can be
	// // scheduled for execution
	// tasks.add(receivedTask);
	// // Task scheduler takes care of scheduling the task in case the execution stopped and the time to live did not yet run
	// // out... This is a treaky one... If we use a TimeToLive to decide if the Procedure is finished
	// // Hmn... Why not use a little average time for that? Always add received message time and use the max waiting time (Plus some
	// // delta) to wait in the scheduler...
	// } else {
	// // The task was already once created or received... So simply update the executors of the task
	// int taskIndex = tasks.indexOf(receivedTask);
	// Task taskToUpdate = tasks.get(taskIndex);
	// if (!taskToUpdate.isFinished()) {
	// if (receivedTask.isFinished()) { // Don't care... this task is finished
	// tasks.set(taskIndex, receivedTask);
	// } else {
	//
	// }
	// }
	// }
	// } else /* if(currentPI.procedureIndex() != receivedPI.procedureIndex()) */ { // Abnormal case, something wrong because the executed
	// // procedure is not the same...
	// if (currentPI.procedureIndex() < receivedCurrentProcedureIndex) { // The current job is outdated
	// // BTW currentProcedureIndex is needed as we don't know how FAR AWAY it is (e.g. current1 = 0, current2 = 2.. So don't
	// // simply use previousProcedure()
	// // Let's hope at least the procedureIndex is in a reasonable range (>=0 and smaller than number of procedures...
	// List<Task> oldTasksOfReceivedJob = receivedJobDummy.procedure(currentPI.procedureIndex()).tasks();
	// // Or get it over DHT (getAll(PROCEDURE_KEYS)) if we clear that data after the procedure finished...
	// if (!oldTasksOfReceivedJob.contains(receivedTask)) {
	// // Well that's good for THIS executor... It executes correctly, meaning that it got a task that was not yet
	// // executed... All the others need to rewind as soon as they notice...
	// } else {
	// // We are outdated!! Need to update to received job
	// handleReceivedJob(receivedJobDummy);
	// }
	// } else if (currentPI.procedureIndex() > receivedCurrentProcedureIndex) { // received message that is presumably outdated
	// // This executor executes a procedure that is further in the processing chain than the received one
	// // Hmn, may be because either the message received is out of date, or there was a task assigned that was not yet assigned
	// // before the procedure in this executor ended...
	// // Or get it over DHT (getAll(PROCEDURE_KEYS)) if we clear that data after the procedure finished...
	// List<Task> oldTasksOfJob = receivedJobDummy.procedure(currentPI.procedureIndex()).tasks();
	// if (!oldTasksOfJob.contains(receivedTask)) {
	// /*
	// * This is crap. That means the received task is from an older procedure and was not yet executed. So the currently
	// * executed procedure needs to be stopped, data removed, and the jobs current procedure index needs to be set back to the
	// * received one... But even better... I don't care here and simply remove it from the list and reschedule...
	// */
	// if (jobExecutor.currentlyExecutedJob() != null && !jobExecutor.currentlyExecutedJob().equals(jobToUpdate)) { // No job is
	// // executed
	// // atm
	//
	// // logger.info("jobExecutor.currentlyExecutedJob() != null");
	// // The job currently executing is not the one with highest priority
	// // anymore
	// // Abort the currently executing job (meaning, interrupt until later resume... This finishes the task executing atm
	// jobExecutor.isExecutionAborted(true);
	//
	// }
	// jobs.remove(jobToUpdate);
	// // jobExecutor.broadcastRemoveData(job); // Let Michigan handle it (... meaning the DHT cleaner process))
	// // Will reschedule the job with the updated procedure...
	// handleReceivedJob(receivedJobDummy);
	// } else {
	// // Nothing to worry, this is just an old message and thus can be discarded...
	// }
	// } else /* if(currentPI.procedureIndex() == receivedPI.procedureIndex()) */ {
	// // Not possible as this case is covered by the previous if else statement (if(currentlyExecutedJob.procedureIndex() ==
	// // job.procedureIndex()))
	// }
	// }
	// } else /* if (!jobToUpdate.isActive()) */ {
	// // Well another job is executing... so simply treat it as new job received and let Michigan handle it (will probably be discarded)
	// handleReceivedJob(receivedJobDummy);
	// }
	// } else /* if(!jobs.contains(job)) */ {
	// // Apparently we have not received this job yet (in case a new executor comes online)...
	// // Treat it as if a new job was received... Which is the same as if the next procedure is executed
	// handleReceivedJob(receivedJobDummy);
	// }
	// }

	// boolean containsJob = false; // Use this to check if eventually, a new job was received that is being executed (in case of a newly online
	// // executor)
	// synchronized (jobs) {
	// logger.info("Looking for job in " + jobs);
	// for (Job job : jobs) {
	// logger.info(job.id() + ".equals(" + jobToUpdate.id() + ")" + job.id().equals(task.jobId()));
	// if (job.equals(jobToUpdate)) {
	// containsJob = true; // Do nothing in the end, we already received that job once
	// logger.info("found job: " + job);
	// Job currentlyExecutedJob = jobExecutor.currentlyExecutedJob();
	// if (currentlyExecutedJob != null) { // Ignore the message if it's only using up space and
	// // syncing time
	// if (job.equals(currentlyExecutedJob)) {
	// List<Task> tasks = job.currentProcedure().tasks();
	// logger.info("found tasks: " + tasks);
	// synchronized (tasks) {
	// int taskIndex = tasks.indexOf(task);
	// if (taskIndex < 0) {// Received a new task currently not yet assigned
	// tasks.add(task);
	// taskIndex = tasks.size() - 1;
	// }
	// logger.info("Updating task: " + tasks.get(taskIndex) + " with " + updateInformation);
	// Tasks.updateStati(tasks.get(taskIndex), updateInformation, job.maxNrOfFinishedWorkersPerTask());
	// break;
	// }
	// } else {
	// logger.warn("Ignored job update as it is not one for the currently executed job");
	// }
	// } else {
	// logger.info("Problem: job found but currently, there is no job executing. Why? Makes no sense");
	// // TODO what could be the reason for this...
	// }
	// }
	// }
	// }
	// // Apparently we received a new job... Especially possible in cases this executor joint into a current execution and received a first status
	// // update as a broadcast
	// if (!containsJob) {
	// handleReceivedJob(jobToUpdate);
	// }

	// public void handleFinishedProcedure(String jobId) {
	// logger.info("handleFinishedProcedure " + job.currentProcedure());
	// // if (jobExecutor.dhtConnectionProvider().peerAddress().equals(sender)) { // sent it to myself... Nothing to do
	// // return;
	// // }
	// if (this.jobExecutor.currentlyExecutedJob() != null && this.jobExecutor.currentlyExecutedJob().equals(job)) {
	// jobExecutor.isExecutionAborted(true); // finished already... doesn't need to be executed anymore
	//
	// // TODO: hmnn... Should I start another job here while we wait for the evaluatiion of this job? I DON'T KNOW... Well let's think this
	// // through... Only if it was the last procedure and the job is finished, else if we take the next procedure of the same job, there isn't
	// // any data to collect for tasks yet (will be done below)... Sooo will think about it...
	//
	// }
	//
	// List<Task> tasks = job.currentProcedure().tasks();
	//
	// List<FutureGet> futureGets = SyncedCollectionProvider.syncedArrayList();
	// List<FuturePut> futurePuts = SyncedCollectionProvider.syncedArrayList();
	// for (Task task : tasks) {
	// logger.info("task: " + task);
	// List<Tuple<String, Integer>> finalExecutorTaskDomainParts = task.finalExecutorTaskDomainParts();
	// // logger.info("finalExecutorTaskDomainParts: " + finalExecutorTaskDomainParts);
	// for (Tuple<String, Integer> finalDataLocationDomain : finalExecutorTaskDomainParts) {
	// // logger.info("finalDataLocationDomain: " + finalDataLocationDomain);
	// Tuple<String, Tuple<String, Integer>> executorTaskDomain = task.executorTaskDomain(finalDataLocationDomain);
	// String combination = task.concatenationString(finalDataLocationDomain);
	// logger.info("get task keys for task executor domain: " + combination);
	// futureGets.add(this.jobExecutor.dhtConnectionProvider().getAll(DomainProvider.TASK_KEYS, combination)
	// .addListener(new BaseFutureAdapter<FutureGet>() {
	//
	// @Override
	// public void operationComplete(FutureGet future) throws Exception {
	// if (future.isSuccess()) {
	// logger.info("Success on retrieving task keys for task executor domain: " + combination);
	// try {
	// // logger.info("future.dataMap() != null: " + (future.dataMap() != null));
	// // if (future.dataMap() != null) {
	// Set<Number640> keySet = future.dataMap().keySet();
	// logger.info("KeySet: " + keySet);
	// for (Number640 n : keySet) {
	// String key = (String) future.dataMap().get(n).object();
	// logger.info("Key: " + key);
	// futurePuts.add(jobExecutor.dhtConnectionProvider().add(key, executorTaskDomain,
	// job.currentProcedure().jobProcedureDomainString(), false));
	// futurePuts.add(jobExecutor.dhtConnectionProvider().add(DomainProvider.PROCEDURE_KEYS, key,
	// job.currentProcedure().jobProcedureDomainString(), false));
	// }
	// // }
	// } catch (IOException e) {
	// logger.warn("IOException on getting the data", e);
	// }
	// } else {
	// logger.info("No success retrieving task keys form task executor domain: " + combination);
	// }
	// }
	// }));
	//
	// }
	//
	// }
	// logger.info("futureGets: " + futureGets);
	// if (futureGets.size() > 0) {
	// Futures.whenAllSuccess(futureGets).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
	//
	// @Override
	// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
	// logger.info("futurePuts: " + futurePuts);
	// if (future.isSuccess()) {
	// Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FuturePut[]>>() {
	//
	// @Override
	// public void operationComplete(FutureDone<FuturePut[]> future) throws Exception {
	//
	// if (future.isSuccess()) {
	// logger.info("Successfully put tasks into jobProcedure domain.JobProcedureDomain: "
	// + job.currentProcedure().jobProcedureDomainString() + ", Tasks: " + tasks);
	// // well... check if there is actually any procedure left... Else job is finished...
	// job.incrementProcedureIndex();
	// ProcedureInformation subsequentProcedure = job.currentProcedure();
	// job.previousProcedure().tasks().clear();
	// if (subsequentProcedure.procedure().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName())) {
	// // Finished job :)
	// logger.info("Finished job");
	// JobFinishedBCMessage message = jobExecutor.dhtConnectionProvider().broadcastFinishedJob(job);
	// bcMessages.add(message);
	// // jobs.remove(job);
	// } else {
	// // Next procedure!!
	// logger.info("Execute next procedure");
	// JobDistributedBCMessage message = jobExecutor.dhtConnectionProvider().broadcastNewJob(job);
	// bcMessages.add(message);
	// logger.info("job.currentProcedure() : " + job.previousProcedure());
	// }
	// } else {
	// // TODO Well... something has to be done instead... am I right?
	// logger.info("No success");
	// }
	// }
	//
	// });
	// } else {
	// logger.info("No success");
	// }
	// }
	//
	// });
	//
	// } else {
	// logger.warn("No FuturePuts created. Check why?");
	// }
	// System.err.println("END");
	// }
	//
	// @Override
	// public void handleFinishedJob(Job job) {
	// logger.info("received job finished message: " + job.id());
	//// jobs.remove(jobs.indexOf(job));
	// }

}
