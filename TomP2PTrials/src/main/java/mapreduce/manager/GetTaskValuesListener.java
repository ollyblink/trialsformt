package mapreduce.manager;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapreduce.execution.ExecutorTaskDomain;
import mapreduce.storage.IDHTConnectionProvider;
import mapreduce.utils.DomainProvider;
import mapreduce.utils.Value;
import net.tomp2p.dht.FutureGet;
import net.tomp2p.futures.BaseFutureAdapter;
import net.tomp2p.peers.Number640;

public class GetTaskValuesListener extends BaseFutureAdapter<FutureGet> {
	private static Logger logger = LoggerFactory.getLogger(GetTaskValuesListener.class);

	private ExecutorTaskDomain inputDomain;
	private List<Object> valuesCollector;
	private IDHTConnectionProvider dhtConnectionProvider;

	private int retrievalCount;

	private int maxNrOfRetrievalTrials;

	public GetTaskValuesListener(ExecutorTaskDomain inputDomain, List<Object> valuesCollector, int retrievalCount, int maxNrOfRetrievalTrials,
			IDHTConnectionProvider dhtConnectionProvider) {
		this.inputDomain = inputDomain;
		this.valuesCollector = valuesCollector;
		this.retrievalCount = retrievalCount;
		this.maxNrOfRetrievalTrials = maxNrOfRetrievalTrials;
		this.dhtConnectionProvider = dhtConnectionProvider;
	}

	@Override
	public void operationComplete(FutureGet future) throws Exception {
		if (future.isSuccess()) {
			try {
				if (future.dataMap() != null) {
					for (Number640 n : future.dataMap().keySet()) {
						valuesCollector.add(((Value) future.dataMap().get(n).object()).value());
					}

				}
			} catch (IOException e) {
				logger.warn("IOException on getting the data", e);
			}
		} else {
			if (retrievalCount++ < maxNrOfRetrievalTrials) {
				dhtConnectionProvider
						.getAll(inputDomain.taskId(), DomainProvider.INSTANCE.concatenation(inputDomain.jobProcedureDomain(), inputDomain))
						.addListener(new GetTaskValuesListener(inputDomain, valuesCollector, retrievalCount, maxNrOfRetrievalTrials,
								dhtConnectionProvider));
			} else {
				logger.warn("No success on retrieving data for " + inputDomain.taskId() + ", tried " + retrievalCount + " times");
			}
		}
	}
}
