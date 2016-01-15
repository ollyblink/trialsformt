package mapreduce.engine.executors;

import mapreduce.storage.IDHTConnectionProvider;

public abstract class AbstractExecutor implements IExecutor {

	protected IDHTConnectionProvider dhtConnectionProvider;
	protected String id;
	
	protected AbstractExecutor(String id){
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

}
