package mapreduce.execution.computation.context;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.job.Job;
import mapreduce.execution.task.Task;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Tuple;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureListener;
import net.tomp2p.peers.Number160;

public class DHTStorageContext extends AbstractBaseContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);

	private IDHTConnectionProvider dhtConnectionProvider;

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
	private DHTStorageContext(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		// ListMultimap<String, Object> multimap = ArrayListMultimap.create();
		// this.tmpKeyValues = Multimaps.synchronizedListMultimap(multimap);
	}

	public static DHTStorageContext create(IDHTConnectionProvider dhtConnectionProvider) {
		return new DHTStorageContext(dhtConnectionProvider);
	}

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (task == null) {
			logger.warn("No task id set!");
			return;
		}

		updateResultHash(keyOut, valueOut);

		String executorTaskDomain = DomainProvider.INSTANCE.executorTaskDomain(task,
				Tuple.create(dhtConnectionProvider.owner(), task.executingPeers().get(dhtConnectionProvider.owner()).size() - 1));

		String combinedExecutorTaskDomain = subsequentJobProcedureDomain + "_" + executorTaskDomain;
		this.dhtConnectionProvider.add(keyOut.toString(), valueOut, combinedExecutorTaskDomain, true)
				.addListener(new BaseFutureListener<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Put <" + keyOut + ", " + valueOut + ">");
						} else {
							logger.warn("Could not put <" + keyOut + ", " + valueOut + ">");
						}
					}

					@Override
					public void exceptionCaught(Throwable t) throws Exception {
						logger.warn("Exception thrown", t);
					}

				});

		
		// }

	}

	// private boolean dataLimitAchieved() {
	//
	// long dataSizes = 0;
	// for (String key : tmpKeyValues.keySet()) {
	// dataSizes += key.getBytes(Charset.forName("UTF-8")).length;
	// for (Object value : tmpKeyValues.get(key)) {
	// dataSizes += value.toString().getBytes(Charset.forName("UTF-8")).length;
	// }
	// }
	// return dataSizes >= maxDataSize.value();
	// }

	public DHTStorageContext task(Task task) {
		this.task = task;
		return this;
	}

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
	public void broadcastResultHash() {
		dhtConnectionProvider.broadcastFinishedTask(task, resultHash());
	}

	@Override
	public String subsequentJobProcedureDomain() {
		return super.subsequentJobProcedureDomain();
	}

	@Override
	public DHTStorageContext subsequentJobProcedureDomain(String jobProcedureDomain) {
		return (DHTStorageContext) super.subsequentJobProcedureDomain(jobProcedureDomain);
	}
}
