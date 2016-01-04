package mapreduce.manager;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.execution.JobProcedureDomain;
import mapreduce.execution.task.Task2;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.SyncedCollectionProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.dht.FuturePut;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.futures.FutureDone;
import net.tomp2p.futures.Futures;
import net.tomp2p.peers.Number640;

public class PutTaskKeysAndValuesOnRetrieval extends BaseFutureAdapter<FutureGet> {
	private static Logger logger = LoggerFactory.getLogger(PutTaskKeysAndValuesOnRetrieval.class);

	private ExecutorTaskDomain inputDomain;
	private IDHTConnectionProvider dhtConnectionProvider;

	private int getCount;
	private int putCount;

	private int maxNrOfRetrievalTrials;

	private String key;

	private JobProcedureDomain outputDomain;

	private Task2 task;

	private List<FuturePut> futureProcedureDomainPuts;

	public PutTaskKeysAndValuesOnRetrieval(Task2 task, String key, List<FuturePut> futureProcedureDomainPuts, ExecutorTaskDomain inputDomain,
			JobProcedureDomain outputDomain, int getCount, int putCount, int maxNrOfRetrievalTrials, IDHTConnectionProvider dhtConnectionProvider) {
		this.task = task;
		this.key = key;
		this.futureProcedureDomainPuts = futureProcedureDomainPuts;
		this.inputDomain = inputDomain;
		this.outputDomain = outputDomain;
		this.getCount = getCount;
		this.maxNrOfRetrievalTrials = maxNrOfRetrievalTrials;
		this.dhtConnectionProvider = dhtConnectionProvider;
	}

	@Override
	public void operationComplete(FutureGet future) throws Exception {
		if (future.isSuccess()) {
			futureProcedureDomainPuts
					.add(dhtConnectionProvider.add(DomainProvider.PROCEDURE_OUTPUT_RESULT_KEYS, key, outputDomain.toString(), false));
			for (Number640 n : future.dataMap().keySet()) {
				futureProcedureDomainPuts
						.add(dhtConnectionProvider.add(key, ((Value) future.dataMap().get(n).object()).value(), outputDomain.toString(), true));
			}

		} else {
			if (getCount++ < maxNrOfRetrievalTrials) {
				dhtConnectionProvider
						.getAll(inputDomain.taskId(), DomainProvider.INSTANCE.concatenation(inputDomain.jobProcedureDomain(), inputDomain))
						.addListener(new PutTaskKeysAndValuesOnRetrieval(task, key, futureProcedureDomainPuts, inputDomain, outputDomain, getCount,
								putCount, maxNrOfRetrievalTrials, dhtConnectionProvider));
			} else {
				logger.warn("No success on retrieving data for " + inputDomain.taskId() + ", tried " + getCount + " times");
			}
		}
	}
}
