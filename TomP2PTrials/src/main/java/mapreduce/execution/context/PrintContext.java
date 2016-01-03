package mapreduce.execution.context;

public class PrintContext extends AbstractBaseContext {

	@Override
	public void write(Object keyOut, Object valueOut) {
		if (this.combiner != null) {
			if (combine) {
				System.out.println("Stored <K,V> pair for task " + task.id() + ": <" + keyOut + ", " + valueOut + ">");
			} else {
				this.keyValues.put(keyOut.toString(), valueOut);
			}
		} else {
			System.out.println("Stored <K,V> pair for task " + task.id() + ": <" + keyOut + ", " + valueOut + ">");
		}

	}

	private PrintContext() {

	}

	public static PrintContext newInstance() {
		return new PrintContext();
	}

}
