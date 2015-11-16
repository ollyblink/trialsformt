package firstdesignidea.execution.computation.reducer;

import firstdesignidea.execution.datadistribution.IReduceDataDistributor;

public class DefaultReducerEngine implements IReducerEngine {

	private IReduceDataDistributor reduceDataDistributor;
	private IReducer<?, ?, ?, ?> reducer;

	private DefaultReducerEngine() {

	}

	public static DefaultReducerEngine newDefaultReducerEngine() {
		return new DefaultReducerEngine();
	}

 
	@Override
	public IReducerEngine reduceDataDistributor(IReduceDataDistributor reduceDataDistributor) {
		this.reduceDataDistributor = reduceDataDistributor;
		return this;
	}

	@Override
	public IReducerEngine reducer(IReducer<?, ?, ?, ?> reducer) {
		this.reducer = reducer;
		return this;
	}

}
