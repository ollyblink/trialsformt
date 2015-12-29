package mapreduce.execution.computation.standardprocedures;

import java.util.Collection;
import java.util.StringTokenizer;

import mapreduce.execution.computation.IMapReduceProcedure;
import mapreduce.execution.computation.context.IContext;

public class WordCountMapper implements IMapReduceProcedure {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4762197473747869364L;

	@Override
	public void process(Object keyIn, Collection<Object> valuesIn, IContext context) {
		for (Object value : valuesIn) {
			String valueString = (String) value;
			StringTokenizer tokens = new StringTokenizer(valueString);
			while (tokens.hasMoreTokens()) {
				context.write(tokens.nextToken(), 1);
			}
		}
	}

	public static IMapReduceProcedure create() {
		return new WordCountMapper();
	}

	private WordCountMapper() {

	}
}
