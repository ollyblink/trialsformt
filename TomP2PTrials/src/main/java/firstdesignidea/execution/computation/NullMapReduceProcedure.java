package firstdesignidea.execution.computation;

import firstdesignidea.execution.computation.context.IContext;

/**
 * Dummy class for testing
 * @author ozihler
 *
 */
public class NullMapReduceProcedure implements IMapReduceProcedure<Object, Object, Object, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2732769543535949510L;

	@Override
	public void process(Object key, Object value, IContext<Object, Object> context) {
		context.write(key, value);
	}

}
