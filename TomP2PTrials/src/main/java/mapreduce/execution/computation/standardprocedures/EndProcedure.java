package mapreduce.execution.computation.standardprocedures;

import java.util.Collection;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;

public class EndProcedure implements IMapReduceProcedure {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3945145812631536461L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		return;
	}

	public static EndProcedure create() {
		return new EndProcedure();
	}

	private EndProcedure() {

	}
}
