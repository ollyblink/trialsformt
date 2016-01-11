package mapreduce.engine.executor;

import static mapreduce.utils.SyncedCollectionProvider.syncedArrayList;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.engine.broadcasting.CompletedBCMessage;
import mapreduce.engine.messageconsumer.MRJobExecutionManagerMessageConsumer;
import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.context.DHTStorageContext;
import mapreduce.execution.context.IContext;
import mapreduce.execution.procedures.Procedure;
import mapreduce.execution.task.Task;
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

public class MRJobExecutionManager {
	private static Logger logger = LoggerFactory.getLogger(MRJobExecutionManager.class);

	private IDHTConnectionProvider dhtCon;
	// private MRJobExecutionManagerMessageConsumer messageConsumer;

	private String id;

	private MRJobExecutionManager() {

		this.id = IDCreator.INSTANCE.createTimeRandomID(getClass().getSimpleName());
	}

	public static MRJobExecutionManager create() {
		return new MRJobExecutionManager();
	}

	public IDHTConnectionProvider dhtConnectionProvider() {
		return this.dhtCon;
	}

	public MRJobExecutionManager dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtCon = dhtConnectionProvider.executor(this.id);
		return this;
	}
	// END GETTER/SETTER

	// Maintenance

	public void shutdown() {
		dhtCon.shutdown();
	}

	public String id() {
		return this.id;
	}

	public void start() {
		// this.dhtConnectionProvider.connect();
	}

	// End Maintenance
	// Execution

	public void executeTask(Task task, Procedure procedure) {
		// if (task.canBeExecuted()) {
		// task.incrementActiveCount();

		logger.info("Task to execute: " + task);
		// Now we actually wanna retrieve the data from the specified locations...
		dhtCon.getAll(task.key(), procedure.inputDomain().toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					List<Object> values = syncedArrayList();
					Set<Number640> valueSet = future.dataMap().keySet();
					for (Number640 valueHash : valueSet) {
						Object taskValue = ((Value) future.dataMap().get(valueHash).object()).value();
						values.add(taskValue);
					}

					logger.info("Executing task: " + task.key() + " with values " + values);
					JobProcedureDomain outputJPD = JobProcedureDomain.create(procedure.inputDomain().jobId(), id,
							procedure.executable().getClass().getSimpleName(), procedure.procedureIndex());

					ExecutorTaskDomain outputETD = ExecutorTaskDomain.create(task.key(), id, task.newStatusIndex(), outputJPD);

					logger.info("Output ExecutorTaskDomain: " + outputETD.toString());
					IContext context = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtCon);
					if (procedure.combiner() != null) {
						IContext combinerContext = DHTStorageContext.create().outputExecutorTaskDomain(outputETD).dhtConnectionProvider(dhtCon);
						context.combiner(procedure.combiner(), combinerContext);
					}
					procedure.executable().process(task.key(), values, context);
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
										procedure.inputDomain().nrOfFinishedTasks(procedure.nrOfFinishedTasks()));
								dhtCon.broadcastHandler().addBCMessage(msg);
								// Adds it to itself, does not receive broadcasts... Makes sure this result is ignored in case another was
								// received
								// already
								dhtCon.broadcastCompletion(msg);
								logger.info("Successfully broadcasted TaskCompletedBCMessage for task " + task);
							} else {
								logger.warn("No success on task execution. Reason: " + future.failedReason());
							}
							// task.decrementActiveCount();
						}

					});
				} else {
					logger.info("Could not retrieve data for task " + task.key() + " in job procedure domain: " + procedure.inputDomain().toString()
							+ ". Failed reason: " + future.failedReason());
					// task.decrementActiveCount();
				}

			}

		});
		// }
	}

	public void switchDataFromTaskToProcedureDomain(Procedure procedure, Task taskToTransfer) {
		if (taskToTransfer.isFinished() && !taskToTransfer.isInProcedureDomain()) {
			logger.info("Transferring tasks " + taskToTransfer + " to procedure domain");

			List<FutureGet> futureGetKeys = syncedArrayList();
			List<FutureGet> futureGetValues = syncedArrayList();
			List<FuturePut> futurePuts = syncedArrayList();

			ExecutorTaskDomain from = (ExecutorTaskDomain) taskToTransfer.resultOutputDomain();
			JobProcedureDomain to = JobProcedureDomain.create(from.jobProcedureDomain().jobId(), id, from.jobProcedureDomain().procedureSimpleName(),
					from.jobProcedureDomain().procedureIndex());
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
												logger.info("Successfully transfered task output keys and values for task " + taskToTransfer
														+ " from task executor domain to job procedure domain: " + to.toString() + ". ");

												// if (procedure.isFinished()) {
												if (procedure.inputDomain().tasksSize() == procedure.tasks().size()) {
													boolean isProcedureCompleted = true;
													for (Task task : procedure.tasks()) {
														if (!task.isFinished()) {
															isProcedureCompleted = false;
														}
													}
													if (isProcedureCompleted) {
														CompletedBCMessage msg = CompletedBCMessage.createCompletedProcedureBCMessage(
																to.resultHash(procedure.calculateResultHash()), procedure.inputDomain());
														dhtCon.broadcastHandler().addBCMessage(msg);
														dhtCon.broadcastCompletion(msg);
													}
												}
											} else {
												logger.warn("Failed to transfered task output keys and values for task " + taskToTransfer
														+ " from task executor domain to job procedure domain: " + to.toString() + ". failed reason: "
														+ future.failedReason());
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

	private void transferDataFromETDtoJPD(Task task, ExecutorTaskDomain fromETD, JobProcedureDomain toJPD, List<FutureGet> futureGetKeys,
			List<FutureGet> futureGetValues, List<FuturePut> futurePuts) {

		futureGetKeys.add(dhtCon.getAll(DomainProvider.TASK_OUTPUT_RESULT_KEYS, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

			@Override
			public void operationComplete(FutureGet future) throws Exception {
				if (future.isSuccess()) {
					Set<Number640> keySet = future.dataMap().keySet();
					for (Number640 n : keySet) {
						String taskOutputKey = (String) future.dataMap().get(n).object();
						futureGetValues.add(dhtCon.getAll(taskOutputKey, fromETD.toString()).addListener(new BaseFutureAdapter<FutureGet>() {

							@Override
							public void operationComplete(FutureGet future) throws Exception {
								if (future.isSuccess()) {
									Collection<Data> values = future.dataMap().values();
									List<Object> realValues = syncedArrayList();
									for (Data d : values) {
										realValues.add(((Value) d.object()).value());
									}

									futurePuts.add(
											dhtCon.addAll(taskOutputKey, values, toJPD.toString()).addListener(new BaseFutureAdapter<FuturePut>() {

										@Override
										public void operationComplete(FuturePut future) throws Exception {

											if (future.isSuccess()) {
												logger.info("Successfully added task output values {" + realValues + "} of task output key \""
														+ taskOutputKey + "\" for task " + task.key() + " to output procedure domain "
														+ toJPD.toString());

											} else {
												logger.info(
														"Failed to add values for task output key " + taskOutputKey + " to output procedure domain "
																+ toJPD.toString() + ", failed reason: " + future.failedReason());
											}
										}

									}));
									futurePuts.add(dhtCon.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, taskOutputKey, toJPD.toString(), false)
											.addListener(new BaseFutureAdapter<FuturePut>() {

										@Override
										public void operationComplete(FuturePut future) throws Exception {
											if (future.isSuccess()) {
												logger.info("Successfully added task output key \"" + taskOutputKey + "\" for task " + task.key()
														+ " to output procedure domain " + toJPD.toString());
											} else {
												logger.info("Failed to add task output key and values for task output key \"" + taskOutputKey
														+ "\" for task " + task.key() + " to output procedure domain " + toJPD.toString()
														+ ", failed reason: " + future.failedReason());
											}
										}

									}));

								} else {
									logger.info("Failed to get task output key and values for task output key (" + taskOutputKey
											+ " from task executor domain " + fromETD.toString() + ", failed reason: " + future.failedReason());

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

	// public MRJobExecutionManagerMessageConsumer messageConsumer() {
	// return messageConsumer;
	// }

	// End Execution

}
