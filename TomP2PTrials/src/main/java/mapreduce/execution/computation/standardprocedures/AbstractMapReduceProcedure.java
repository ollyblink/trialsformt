package mapreduce.execution.computation.standardprocedures;

import mapreduce.execution.computation.IMapReduceProcedure;

public abstract class AbstractMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> implements IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3307844612875807324L;
	private Integer procedureNr;

	@Override
	public AbstractMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> procedureNr(Integer procedureNr) {
		this.procedureNr = procedureNr;
		return this;
	}

	@Override
	public int compareTo(IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT> o) {
		return procedureNr.compareTo(o.procedureNr());
	}

	@Override
	public Integer procedureNr() {
		return procedureNr;
	}

}
