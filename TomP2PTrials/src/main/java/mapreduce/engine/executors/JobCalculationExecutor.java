package mapreduce.engine.executors;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.messages.CompletedBCMessage;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.domains.ExecutorTaskDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.procedures.IExecutable;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.tasks.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.IDCreator;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;
import net.tomp2p.storage.Data;

public class JobCalculationExecutor extends AbstractExecutor {
	private static Logger logger = LoggerFactory.getLogger(JobCalculationExecutor.class);

	// private Map<String, ListMultimap<Task, BaseFuture>> futures;

	private JobCalculationExecutor() {
		super(IDCreator.INSTANCE.createTimeRandomID(JobCalculationExecutor.class.getSimpleName()));
	}

	public static JobCalculationExecutor create() {
		return new JobCalculationExecutor();
	}

	public void executeTask(Task task, Procedure procedure) {
		logger.info("executeTask: Task to execute: " + task);
		// Now we actually wanna retrieve the data from the specified locations...
		// ListMultimap<Task, BaseFuture> listMultimap = getMultimap(procedure);
		// listMultimap.put(task,
		dhtConnectionProvider.getAll(task.key(), procedure.dataInputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					List<Object> values = syncedArrayList();
					Set<Number640> valueSet = future.dataMap().keySet();
					for (Number640 valueHash : valueSet) {
						Object taskValue = ((Value) future.dataMap().get(valueHash).object()).value();
						values.add(taskValue);
					}

					JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id,
							procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());

					ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.newStatusIndex(), outputJPD);

					// logger.info("executeTask: outputJPD: " + outputJPD.toString() + ", outputETD: " + outputETD.toString() + ", procedure:
					// "
					// + procedure);
					IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtConnectionProvider);
					if (procedure.combiner() != null) {
						IContext combinerContext = DHTStorageContext.create().outputExecutorTaskDomain(outputETD)
								.dhtConnectionProvider(dhtConnectionProvider);
						context.combiner((IExecutable) procedure.combiner(), combinerContext);
					}
					((IExecutable) procedure.executable()).process(task.key(), values, context);
					if (procedure.combiner() != null) {
						context.combine();
					}
					IContext contextToUse = (procedure.combiner() == null ? context : context.combinerContext());
					Futures.whenAllSuccess(contextToUse.futurePutData()).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {
						@Override
						public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
							if (future.isSuccess()) {
								outputETD.resultHash(contextToUse.resultHash());
								CompletedBCMessage msg = CompletedBCMessage.createCompletedTaskBCMessage(outputETD,
										procedure.dataInputDomain().nrOfFinishedTasks(procedure.nrOfFinishedTasks()));
								// dhtCon.broadcastHandler().addBCMessage(msg);
								dhtConnectionProvider.broadcastHandler().processMessage(msg,
										dhtConnectionProvider.broadcastHandler().getJob(outputJPD.jobId()));
								// Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in case another was
								// received
								// already
								dhtConnectionProvider.broadcastCompletion(msg);
								logger.info("executeTask: Successfully broadcasted TaskCompletedBCMessage for task " + task);
							} else {
								logger.warn("executeTask: No success on task execution. Reason: " + future.failedReason());
							}
							// task.decrementActiveCount();
						}

					});
				} else {
					logger.info("Could not retrieve data for task " + task.key() + " in job procedure domain: "
							+ procedure.dataInputDomain().toString() + ". Failed reason: " + future.failedReason());
					// task.decrementActiveCount();
				}

			}

		})
		// )
		;
		// }
	}

	public void switchDataFromTaskToProcedureDomain(Procedure procedure, Task taskToTransfer) {
		if (taskToTransfer.isFinished() && !taskToTransfer.isInProcedureDomain()) {
			logger.info("switchDataFromTaskToProcedureDomain: Transferring tasks " + taskToTransfer + " to procedure domain");

			List<FutureGet> futureGetKeys = syncedArrayList();
			List<FutureGet> futureGetValues = syncedArrayList();
			List<FuturePut> futurePuts = syncedArrayList();

			ExecutorTaskDomain from = (ExecutorTaskDomain) taskToTransfer.resultOutputDomain();

			JobProcedureDomain to = JobProcedureDomain.create(procedure.jobId(), procedure.dataInputDomain().jobSubmissionCount(), id,
					procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
			transferDataFromETDtoJPD(taskToTransfer, from, to, futureGetKeys, futureGetValues, futurePuts);
			Futures.whenAllSuccess(futureGetKeys).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

				@Override
				public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
					if (future.isSuccess()) {
						Futures.whenAllSuccess(futureGetValues).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

							@Override
							public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
								if (future.isSuccess()) {
									Futures.whenAllSuccess(futurePuts).addListener(new BaseFutureAdapter<FutureDone<FutureGet[]>>() {

										@Override
										public void operationComplete(FutureDone<FutureGet[]> future) throws Exception {
											if (future.isSuccess()) {
												taskToTransfer.isInProcedureDomain(true);
												logger.info(
														"switchDataFromTaskToProcedureDomain: Successfully transfered task output keys and values for task "
																+ taskToTransfer + " from task executor domain to job procedure domain: "
																+ to.toString() + ". ");

												CompletedBCMessage msg = tryFinishProcedure(procedure);
												if (msg != null) { 
													dhtConnectionProvider.broadcastHandler().processMessage(msg,
															dhtConnectionProvider.broadcastHandler().getJob(procedure.jobId()));
													dhtConnectionProvider.broadcastCompletion(msg);
													logger.info("tryFinishProcedure: Broadcasted Completed Procedure MSG: " + msg);
												}
											} else {
												logger.warn(
														"switchDataFromTaskToProcedureDomain: Failed to transfered task output keys and values for task "
																+ taskToTransfer + " from task executor domain to job procedure domain: "
																+ to.toString() + ". failed reason: " + future.failedReason());
											}
										}

									});
								} else {
									logger.warn("Failed to get task values for task " + taskToTransfer + " from task executor domain. failed reason: "
											+ future.failedReason());
								}
							}

						});
					} else {
						logger.warn("Failed to get task keys for task " + taskToTransfer + " from task executor domain. failed reason: "
								+ future.failedReason());
					}
				}

			});
		}
	}

	public CompletedBCMessage tryFinishProcedure(Procedure procedure) {
		JobProcedureDomain dataInputDomain = procedure.dataInputDomain();
		int expectedSize = dataInputDomain.expectedNrOfFiles();
		List<Task> tasks = procedure.tasks();
		int currentSize = tasks.size();
		logger.info("Tasks: " + tasks);
		logger.info("tryFinishProcedure: data input domain procedure: " + dataInputDomain.procedureSimpleName());
		logger.info("tryFinishProcedure: expectedSize == currentSize? " + expectedSize + "==" + currentSize);
		if (expectedSize == currentSize) {
			boolean isProcedureCompleted = true;
			synchronized (tasks) {
				for (Task task : tasks) {
					if (!task.isFinished() || !task.isInProcedureDomain()) {
						isProcedureCompleted = false;
					}
				}
			}
			logger.info("tryFinishProcedure: isProcedureCompleted: " + isProcedureCompleted);
			if (isProcedureCompleted) {
				JobProcedureDomain to = JobProcedureDomain.create(procedure.jobId(), dataInputDomain.jobSubmissionCount(), id,
						procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());
				CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(to.resultHash(procedure.calculateResultHash()),
						dataInputDomain);
				return msg;
			}
		}
		return null;
	}

	private void transferDataFromETDtoJPD(Task task, ExecutorTaskDomain fromETD, JobProcedureDomain toJPD, List<FutureGet> futureGetKeys,
			List<FutureGet> futureGetValues, List<FuturePut> futurePuts) {

		futureGetKeys.add(dhtConnectionProvider.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, fromETD.toString())
				.addListener(new BaseFutureAdapter<FutureGet>() {

					@Override
					public void operationComplete(FutureGet future) throws Exception {
						if (future.isSuccess()) {
							Set<Number640> keySet = future.dataMap().keySet();
							for (Number640 n : keySet) {
								String taskOutputKey = (String) future.dataMap().get(n).object();
								futureGetValues.add(dhtConnectionProvider.getAll(taskOutputKey, fromETD.toString())
										.addListener(new BaseFutureAdapter<FutureGet>() {

									@Override
									public void operationComplete(FutureGet future) throws Exception {
										if (future.isSuccess()) {
											Collection<Data> values = future.dataMap().values();
											List<Object> realValues = syncedArrayList();
											for (Data d : values) {
												realValues.add(((Value) d.object()).value());
											}

											futurePuts.add(dhtConnectionProvider.addAll(taskOutputKey, values, toJPD.toString())
													.addListener(new BaseFutureAdapter<FuturePut>() {

												@Override
												public void operationComplete(FuturePut future) throws Exception {

													if (future.isSuccess()) {
														logger.info("Successfully added task output values {" + realValues + "} of task output key \""
																+ taskOutputKey + "\" for task " + task.key() + " to output procedure domain "
																+ toJPD.toString());

													} else {
														logger.info("Failed to add values for task output key " + taskOutputKey
																+ " to output procedure domain " + toJPD.toString() + ", failed reason: "
																+ future.failedReason());
													}
												}

											}));
											futurePuts.add(dhtConnectionProvider
													.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey, toJPD.toString(), false)
													.addListener(new BaseFutureAdapter<FuturePut>() {

												@Override
												public void operationComplete(FuturePut future) throws Exception {
													if (future.isSuccess()) {
														logger.info("Successfully added task output key \"" + taskOutputKey + "\" for task "
																+ task.key() + " to output procedure domain " + toJPD.toString());
													} else {
														logger.info("Failed to add task output key and values for task output key \"" + taskOutputKey
																+ "\" for task " + task.key() + " to output procedure domain " + toJPD.toString()
																+ ", failed reason: " + future.failedReason());
													}
												}

											}));

										} else {
											logger.info("Failed to get task output key and values for task output key (" + taskOutputKey
													+ " from task executor domain " + fromETD.toString() + ", failed reason: "
													+ future.failedReason());

										}
									}
								}));

							}
						} else {
							logger.warn("Failed to get task keys for task " + task.key() + " from task executor domain " + fromETD.toString()
									+ ", failed reason: " + future.failedReason());
						}
					}
				}));

	}

	@Override
	public JobCalculationExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		return (JobCalculationExecutor) super.dhtConnectionProvider(dhtConnectionProvider);
	}

}
