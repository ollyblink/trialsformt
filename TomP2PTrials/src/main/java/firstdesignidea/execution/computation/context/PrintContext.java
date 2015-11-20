package firstdesignidea.execution.computation.context;

public class PrintContext implements IContext {

	@Override
	public void write(Object keyOut, Object valueOut) {
		System.out.println("<" + keyOut + ", " + valueOut + ">");
	}

}
