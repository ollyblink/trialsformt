package mapreduce.execution.computation.standardprocedures;

import java.util.StringTokenizer;

import mapreduce.execution.computation.context.IContext;

public class WordCountMapper extends AbstractMapReduceProcedure<String, String, String, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4762197473747869364L;

	@Override
	public void process(String key, String value, IContext context) {
		StringTokenizer tokens = new StringTokenizer(value);
		while (tokens.hasMoreTokens()) {
			context.write(tokens.nextToken(), 1);
		}

	}

	@Override
	public String toString() {
		return "WordCountMapper";
	}
}
