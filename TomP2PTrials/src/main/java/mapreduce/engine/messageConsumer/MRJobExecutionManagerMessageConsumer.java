package mapreduce.engine.messageConsumer;

import java.util.List;

import mapreduce.engine.executor.MRJobExecutionManager;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.IDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.job.Job;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
import mapreduce.utils.SyncedCollectionProvider;

public class MRJobExecutionManagerMessageConsumer extends AbstractMessageConsumer {

	/** Only used to distinguish if its a completed procedure or task to update */
	private interface IUpdate {
		public void executeUpdate(IDomain outputDomain, Procedure currentProcedure);
	}

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
	public void handleCompletedTask(ExecutorTaskDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(outputDomain, inputDomain, new IUpdate() {

			@Override
			public void executeUpdate(IDomain outputDomain, Procedure procedure) {
				ExecutorTaskDomain outputETDomain = (ExecutorTaskDomain) outputDomain;
				Task receivedTask = Task.create(outputETDomain.taskId());
				List<Task> tasksToUpdate = procedure.tasks();
				if (tasksToUpdate.contains(receivedTask)) {
					logger.info("if (tasksToUpdate.contains(receivedTask))");
					Task thisTask = tasksToUpdate.get(tasksToUpdate.indexOf(receivedTask));
					if (!thisTask.isFinished()) {
						logger.info("if (!thisTask.isFinished())");
						thisTask.addOutputDomain(outputDomain);
						if (thisTask.isFinished()) { // May be finished now...
							logger.info("if (thisTask.isFinished())");
							if (thisTask.isActive()) { // Currently executing... needs abortion and cleanup
								logger.info("if (thisTask.isActive())");
								jobExecutor.abortExecution();
							}
							// Transfer data to procedure domain!
							if (!thisTask.isInProcedureDomain()) {
								logger.info("if (!thisTask.isInProcedureDomain())");
								JobProcedureDomain outputJobProcedureDomain = JobProcedureDomain.create(inputDomain.jobId(), jobExecutor.id(),
										procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
								List<Task> tasksToTransfer = SyncedCollectionProvider.syncedArrayList();
								tasksToTransfer.add(thisTask);
								jobExecutor.transferData(jobs.get(getJob(inputDomain.jobId())), tasksToTransfer, outputJobProcedureDomain,
										inputDomain);
							}
							logger.info("After transfer");
						}
					}
				} else {
					logger.info("else if(!tasksToUpdate.contains(receivedTask))");
					receivedTask.addOutputDomain(outputDomain);
					procedure.addTask(receivedTask);
				}
				logger.info("Before executig job");
				jobExecutor.executeJob(jobs.firstKey());
			}

		});

	}

	@Override
	public void handleCompletedProcedure(JobProcedureDomain outputDomain, JobProcedureDomain inputDomain) {
		handleReceivedMessage(outputDomain, inputDomain, new IUpdate() {
			@Override
			public void executeUpdate(IDomain outputDomain, Procedure procedure) {
				JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;
				logger.info("if (currentProcedure.inputDomain().equals(inputDomain))");
				procedure.addOutputDomain(outputDomain);
				logger.info("procedure.isFinished()? " + procedure.isFinished());
				if (procedure.isFinished()) {
					logger.info("if (currentProcedure.isFinished())");
					if (procedure.isActive()) {
						logger.info("if (currentProcedure.isActive())");
						jobExecutor.abortExecution();
					}
					Job jobToIncrement = getJob(outputJPD.jobId());
					if (jobToIncrement != null) {
						jobToIncrement.incrementProcedureIndex();
						jobToIncrement.currentProcedure().inputDomain(outputJPD);
					}
					jobs.get(getJob(inputDomain.jobId())).clear();

					jobExecutor.executeJob(jobs.firstKey());
				}
			}
		});
	}

	private void handleReceivedMessage(IDomain outputDomain, JobProcedureDomain inputDomain, IUpdate iUpdate) {
		Procedure currentProcedure = jobs.firstKey().currentProcedure();
		if (currentProcedure.procedureIndex() == outputDomain.procedureIndex()) { // Same procedure is executed
			logger.info("if (currentProcedure.procedureIndex() == outputDomain.procedureIndex())");
			if (currentProcedure.inputDomain().equals(inputDomain)) {
				iUpdate.executeUpdate(outputDomain, currentProcedure);
			} else { // May have to change input data location (inputDomain)
				logger.info("else (if (!currentProcedure.inputDomain().equals(inputDomain)))");
				// executor of received message executes on different input data! Need to synchronize
				// Simply compare which one was earlier
				if (currentProcedure.nrOfFinishedTasks() < inputDomain.nrOfFinishedTasks()) {
					logger.info("if (currentProcedure.nrOfFinishedTasks() < outputDomain.nrOfFinishedTasks())");
					// We have completed fewer tasks with our data set than the incoming... abort us and use the incoming data set location instead
					if (currentProcedure.isActive()) {
						logger.info("if (currentProcedure.isActive())");
						jobExecutor.abortExecution();
					}
					currentProcedure.inputDomain(inputDomain);
					currentProcedure.addOutputDomain(outputDomain);
					this.jobExecutor.executeJob(jobs.firstKey());
				}
				// else {// if (currentProcedure.nrOfFinishedTasks() >= outputDomain.nrOfFinishedTasks())
				// // Do nothing... Continue execution with current data set
				// }

			}
		} else if (currentProcedure.procedureIndex() < outputDomain.procedureIndex()) {
			logger.info("if (currentProcedure.procedureIndex() < outputDomain.procedureIndex())");
			// Means this executor is behind in the execution than the one that sent this message
			if (currentProcedure.isActive()) {
				logger.info("if (currentProcedure.isActive())");
				this.jobExecutor.executeJob(jobs.firstKey());
			}
			while (currentProcedure.procedureIndex() < outputDomain.procedureIndex()) {
				logger.info("while(currentProcedure.procedureIndex() < outputDomain.procedureIndex())");
				jobs.firstKey().incrementProcedureIndex();
			}
			currentProcedure.inputDomain(inputDomain);
			currentProcedure.addOutputDomain(outputDomain);
			this.jobExecutor.executeJob(jobs.firstKey());
		}
		// else { // if(currentJob.currentProcedure().procedureIndex() > outputDomain.procedureIndex)
		// // Means this executor is further in the execution than the one that sent this message
		// // Ignore... The other executor will notice this and abort its execution hopefully...
		// }
	}

}
