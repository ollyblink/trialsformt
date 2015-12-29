package mapreduce.execution.computation.standardprocedures;

import java.util.Collection;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;

public class Start implements IMapReduceProcedure {

	/**
	 * 
	 */
	private static final long serialVersionUID = 618605646180541532L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		return;
	}

	public static Start create() {
		return new Start();
	}

	private Start() {

	}

}
