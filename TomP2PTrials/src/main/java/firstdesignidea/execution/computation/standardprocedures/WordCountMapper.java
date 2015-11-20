package firstdesignidea.execution.computation.standardprocedures;

import firstdesignidea.execution.computation.IMapReduceProcedure;
import firstdesignidea.execution.computation.context.IContext;

public class WordCountMapper implements IMapReduceProcedure<String, String, String, Integer> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4762197473747869364L;

	@Override
	public void process(String key, String value, IContext context) {
		String[] words = value.split(" ");
		for (String word : words) {
			context.write(word, 1);
		}

	}

}
