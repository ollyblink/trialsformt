package mapreduce.execution.computation.standardprocedures;

import java.util.Collection;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;

/**
 * Dummy class for testing
 * 
 * @author ozihler
 *
 */
public class NullMapReduceProcedure implements IMapReduceProcedure {

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

}
