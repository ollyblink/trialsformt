package mapreduce.engine.executors;

import mapreduce.storage.IDHTConnectionProvider;

public interface IExecutor {

	public String id();

	public IExecutor dhtConnectionProvider(IDHTConnectionProvider dhtConnectionProvider);

}
