package mapreduce.execution.procedures;

import java.util.Collection;

import mapreduce.execution.context.IContext;

public class WordCountReducer implements IExecutable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6674686068934510011L;

	public static IExecutable create() {
		return new WordCountReducer();
	}

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		int sum = 0;
		for (Object o : valuesIn) {
			sum += (Integer) o;
		}
		context.write(keyIn, sum);
	}

}
