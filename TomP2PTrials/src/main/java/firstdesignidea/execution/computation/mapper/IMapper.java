package firstdesignidea.execution.computation.mapper;

import java.io.Serializable;

import firstdesignidea.execution.computation.context.IContext;

public interface IMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>  extends Serializable{
	public void map(KEYIN key, VALUEIN value, IContext<KEYOUT, VALUEOUT> context);
}
