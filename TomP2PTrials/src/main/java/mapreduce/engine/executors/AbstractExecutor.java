package mapreduce.engine.executors;

import mapreduce.engine.executors.performance.PerformanceInfo;
import mapreduce.storage.IDHTConnectionProvider;

public abstract class AbstractExecutor implements IExecutor {

	protected IDHTConnectionProvider dhtConnectionProvider;
	protected String id;
	private PerformanceInfo performanceInformation;

	protected AbstractExecutor(String id) {
		this.id = id;
	}

	@Override
	public String id() {
		return id;
	}

	@Override
	public IExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider) {
		this.dhtConnectionProvider = dhtConnectionProvider;
		return this;
	}

	@Override
	public PerformanceInfo performanceInformation() {
		return this.performanceInformation;
	}

	@Override
	public AbstractExecutor performanceInformation(PerformanceInfo performanceInformation) {
		this.performanceInformation = performanceInformation;
		return this;
	}

}
