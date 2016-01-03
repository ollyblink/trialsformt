package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

/**
 * Dummy class for testing
 * 
 * @author ozihler
 *
 */
public class NullMapReduceProcedure implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2732769543535949510L;

	@Override
	public void process(Object key, Collection<Object> values, IContext context) {
		context.write(key, values);
	}

	@Override
	public String toString() {
		return "NullMapReduceProcedure";
	}

	private NullMapReduceProcedure() {
	}

	public static IExecutable newInstance() {
		// TODO Auto-generated method stub
		return new NullMapReduceProcedure();
	}

}
