package firstdesignidea.execution.computation.reducer;

import firstdesignidea.execution.datadistribution.IReduceDataDistributor;

public interface IReducerEngine {

	public IReducerEngine reduceDataDistributor(IReduceDataDistributor reduceDataDistributor);

	public IReducerEngine reducer(IReducer<?, ?, ?, ?> reducer);

}
