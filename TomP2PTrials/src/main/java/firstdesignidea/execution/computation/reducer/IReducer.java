package firstdesignidea.execution.computation.reducer;

import firstdesignidea.execution.computation.context.IContext;

public interface IReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
	public void reduce(KEYIN key, Iterable<VALUEIN> values, IContext<KEYOUT, VALUEOUT> context);
}
