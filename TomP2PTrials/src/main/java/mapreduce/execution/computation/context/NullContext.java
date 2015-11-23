package mapreduce.execution.computation.context;

public class NullContext implements IContext {

	@Override
	public void write(Object keyOut, Object valueOut) {

	}

	private NullContext() {

	}

	public static NullContext newNullContext() {
		return new NullContext();
	}

}
