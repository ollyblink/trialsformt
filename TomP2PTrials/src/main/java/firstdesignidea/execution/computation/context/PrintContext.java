package firstdesignidea.execution.computation.context;

public class PrintContext<KEYOUT, VALUEOUT> implements IContext<KEYOUT, VALUEOUT> {

	@Override
	public void write(KEYOUT keyOut, VALUEOUT valueOut) {
		System.out.println("<" + keyOut + ", " + valueOut + ">");
	}

}
