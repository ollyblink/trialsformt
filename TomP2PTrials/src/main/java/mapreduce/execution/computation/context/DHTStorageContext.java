package mapreduce.execution.computation.context;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.task.Task;
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
		updateResultHash(keyOut, valueOut);

		String combinedExecutorTaskDomain = task.concatenationString(taskExecutor);

		this.futurePutData.add(this.dhtConnectionProvider.add(keyOut.toString(), valueOut, combinedExecutorTaskDomain, true)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Successfully performed add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain("
									+ combinedExecutorTaskDomain + ")");
						} else {
							logger.info("Failed to perform add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain("
									+ combinedExecutorTaskDomain + ")");
						}
					}
				}));
		this.futurePutData.add(this.dhtConnectionProvider.add(DomainProvider.TASK_KEYS, keyOut.toString(), combinedExecutorTaskDomain, false)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info(
									"Successfully performed add(TASK_KEYS, " + keyOut.toString() + ").domain(" + combinedExecutorTaskDomain + ")");
						} else {

							logger.warn("Failed to perform add(TASK_KEYS, " + keyOut.toString() + ").domain(" + combinedExecutorTaskDomain + ")");
						}
					}
				}));

	}

	@Override
	public DHTStorageContext taskExecutor(Tuple<String, Integer> taskExecutor) {
		this.taskExecutor = taskExecutor;
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
	public DHTStorageContext combiner(IMapReduceProcedure combiner) {
		this.combiner = combiner;
		return this;
	}

	@Override
	public IMapReduceProcedure combiner() {
		return this.combiner;
	}

	@Override
	public List<FuturePut> futurePutData() {
		return this.futurePutData;
	}

	@Override
	public AbstractBaseContext task(Task task) {
		super.task(task);
		return this;
	}

}
