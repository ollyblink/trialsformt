package firstdesignidea.execution.computation;

import java.io.Serializable;

import firstdesignidea.execution.computation.context.IContext;

public interface IMapReduceProcedure<KEYIN, VALUEIN, KEYOUT, VALUEOUT>  extends Serializable{
	public void process(KEYIN key, VALUEIN value, IContext<KEYOUT, VALUEOUT> context);
}
