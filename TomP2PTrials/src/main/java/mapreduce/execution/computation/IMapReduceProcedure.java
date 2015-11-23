package mapreduce.execution.computation;

import java.io.Serializable;

import mapreduce.execution.computation.context.IContext;

public interface IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Serializable, Comparable<IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT>> {
	public void process(KEYIN key, VALUEIN value, IContext context);
	/**
	 * Used to order which procedure comes before which other procedure
	 * @param procedureNr
	 * @return
	 */
	public IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> procedureNr(Integer procedureNr);
	public Integer procedureNr();
	
}
