package firstdesignidea.execution.computation;

import firstdesignidea.execution.computation.context.IContext;

public class NullMapReduceProcedure implements IMapReduceProcedure<Object, Object, Object, Object> {

	@Override
	public void process(Object key, Object value, IContext<Object, Object> context) {
		context.write(key, value);
	}

}
