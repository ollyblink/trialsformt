package mapreduce.execution.context;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number160;

public class DHTStorageContext implements IContext {
	private static Logger logger = LoggerFactory.getLogger(DHTStorageContext.class);
	// private IExecutable combiner;
	private Number160 resultHash = Number160.ZERO;
	private IDHTConnectionProvider dhtConnectionProvider;
	private List<FuturePut> futurePutData = SyncedCollectionProvider.syncedArrayList();
	private ExecutorTaskDomain outputExecutorTaskDomain;

	/**
	 * 
	 * @param dhtConnectionProvider
	 * @param taskResultComparator
	 *            may add certain speed ups such that the task result comparison afterwards becomes faster
	 */
	private DHTStorageContext() {
	}

	public static DHTStorageContext create() {
		return new DHTStorageContext();
	}

	@Override
	public void write(Object keyOut, Object valueOut) {
		updateResultHash(keyOut, valueOut);

		String oETDString = outputExecutorTaskDomain.toString();
		this.futurePutData
				.add(this.dhtConnectionProvider.add(keyOut.toString(), valueOut, oETDString, true).addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Procedure: " + outputExecutorTaskDomain.jobProcedureDomain().procedureSimpleName()
									+ ": Successfully performed add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString
									+ ")");
						} else {
							logger.info("Procedure: " + outputExecutorTaskDomain.jobProcedureDomain().procedureSimpleName()
									+ ": Failed to perform add(" + keyOut.toString() + ", " + valueOut.toString() + ").domain(" + oETDString + ")");
						}
					}
				}));
		this.futurePutData.add(this.dhtConnectionProvider.add(DomainProvider.TASK_OUTPUT_RESULT_KEYS, keyOut.toString(), oETDString, false)
				.addListener(new BaseFutureAdapter<FuturePut>() {

					@Override
					public void operationComplete(FuturePut future) throws Exception {
						if (future.isSuccess()) {
							logger.info("Procedure: " + outputExecutorTaskDomain.jobProcedureDomain().procedureSimpleName()
									+ ": Successfully performed add(" + DomainProvider.TASK_OUTPUT_RESULT_KEYS + ", " + keyOut.toString()
									+ ").domain(" + oETDString + ")");
						} else {

							logger.warn(
									"Procedure: " + outputExecutorTaskDomain.jobProcedureDomain().procedureSimpleName() + ": Failed to perform add("
											+ DomainProvider.TASK_OUTPUT_RESULT_KEYS + ", " + keyOut.toString() + ").domain(" + oETDString + ")");
						}
					}
				}));

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
	public List<FuturePut> futurePutData() {
		return this.futurePutData;
	}

	@Override
	public DHTStorageContext outputExecutorTaskDomain(ExecutorTaskDomain outputExecutorTaskDomain) {
		this.outputExecutorTaskDomain = outputExecutorTaskDomain;
		return this;
	}

	// @Override
	// public IContext combiner(IExecutable combiner) {
	// this.combiner = combiner;
	// return this;
	// }
	//
	// @Override
	// public IExecutable combiner() {
	// return this.combiner;
	// }

	private void updateResultHash(Object keyOut, Object valueOut) {
		resultHash.xor(Number160.createHash(keyOut.toString())).xor(Number160.createHash(valueOut.toString()));
	}

}
