package firstdesignidea.execution.computation.context;

public interface IContext<KEYOUT, VALUEOUT> {

	public void write(KEYOUT keyOut, VALUEOUT valueOut);

}
