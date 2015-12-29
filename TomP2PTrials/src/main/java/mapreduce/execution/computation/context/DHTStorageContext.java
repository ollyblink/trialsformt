package mapreduce.execution.computation.context;

import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.TaskUpdateBCMessage;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number160;

public class DHTStorageContext extends AbstractBaseContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;

	private List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();

	private Tuple<String, Integer> taskExecutor;

	// private ListMultimap<String, Object> tmpKeyValues;
	//
	// private FileSize maxDataSize = FileSize.FOUR_KILO_BYTES;

	// private ITaskResultComparator taskResultComparator;

	/**
	 * 
	 * @param dhtConnectionProvider
	 * @param taskResultComparator
	 *            may add certain speed ups such that the task result comparison afterwards becomes faster
	 */
	private DHTStorageContext() {
		// ListMultimap<String, Object> multimap = ArrayListMultimap.create();
		// this.tmpKeyValues = Multimaps.synchronizedListMultimap(multimap);
	}

	public static DHTStorageContext create() {
		return new DHTStorageContext();
	}

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (task == null) {
			logger.warn("No task id set!");
			return;
		}

		updateResultHash(keyOut, valueOut);

		String producer = dhtConnectionProvider.owner();
		Integer location = task.executingPeers().get(dhtConnectionProvider.owner()).size() - 1;
		Task newTask = Task.create(keyOut, subsequentJobProcedureDomain);

		String executorTaskDomain = task.concatenationString(Tuple.create(producer, location));

		String combinedExecutorTaskDomain = subsequentJobProcedureDomain + "_" + executorTaskDomain;
		List<FuturePut> futureProcedureKeys = SyncedCollectionProvider.syncedArrayList();
		this.futurePutData.add(this.dhtConnectionProvider.add(keyOut.toString(), valueOut, combinedExecutorTaskDomain, true)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Put <" + keyOut + ", " + valueOut + ">");
							futureProcedureKeys
									.add(dhtConnectionProvider.add(DomainProvider.TASK_KEYS, keyOut.toString(), combinedExecutorTaskDomain, false)
											.addListener(new BaseFutureAdapter<FuturePut>() {

								@Override
								public void operationComplete(FuturePut future) throws Exception {
									if (future.isSuccess()) {
										logger.info("Put <" + keyOut + ", " + valueOut + ">");

									} else {
										logger.warn("Could not put <" + keyOut + ", " + valueOut + ">");
									}
								}

							}));

						} else {
							logger.warn("Could not put <" + keyOut + ", " + valueOut + ">");
						}
					}

				}));

		Futures.whenAllSuccess(futureProcedureKeys).addListener(new BaseFutureAdapter<FuturePut>() {

			@Override
			public void operationComplete(FuturePut future) throws Exception {
				if (future.isSuccess()) {
					logger.info("Put all key");

				} else {
					logger.warn("Could not put key");
				}
			}

		});
		// }

	}

	@Override
	public DHTStorageContext taskExecutor(Tuple<String, Integer> taskExecutor) {
		this.taskExecutor = taskExecutor;
		return this;
	}

	@Override
	public DHTStorageContext task(Task task) {
		this.task = task;
		return this;
	}

	@Override
	public DHTStorageContext dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public Number160 resultHash() {
		return this.resultHash;
	}

	@Override
	public void resetResultHash() {
		this.resultHash = Number160.ZERO;
	}

	@Override
	public DHTStorageContext combiner(IMapReduceProcedure combiner) {
		this.combiner = combiner;
		return this;
	}

	@Override
	public IMapReduceProcedure combiner() {
		return this.combiner;
	}

	@Override
	public Set<Object> keys() {
		return this.keys;
	}

	@Override
	public TaskUpdateBCMessage broadcastResultHash() {
		logger.info("Broadcast result ");
		return dhtConnectionProvider.broadcastFinishedTask(task, taskExecutor, resultHash());
	}

	@Override
	public DHTStorageContext subsequentJobProcedureDomain(Tuple<String, Tuple<String, Integer>> subsequentJobProcedureDomain) {
		return (DHTStorageContext) super.subsequentJobProcedureDomain(subsequentJobProcedureDomain);
	}

	@Override
	public List<FuturePut> futurePutData() {
		return this.futurePutData;
	}
}
