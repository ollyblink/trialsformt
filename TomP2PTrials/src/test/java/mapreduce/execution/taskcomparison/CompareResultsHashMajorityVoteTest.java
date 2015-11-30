package mapreduce.execution.taskcomparison;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.taskcomparison.HashTaskResultComparisonStrategy;
import mapreduce.execution.taskcomparison.ITaskResultComparisonStrategy;

public class CompareResultsHashMajorityVoteTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void test() {
//		ITaskResultComparisonStrategy<> comparator = HashTaskResultComparisonStrategy.newInstance();

		List<String> results = new ArrayList<String>();

		results.add("1 1 1 1 1");
		results.add("1 1 1 1 1 1");
		results.add("1 1 1 1");
		results.add("1 1 1 1 1 1");
		results.add("1 1 1 1");
		
//		comparator.evaluateFinalResult();

		Multimap<String, Integer> hash = ArrayListMultimap.create();
		for (String s : results) {
			hash.put(s, 1);
		}

		for (String s : hash.keySet()) {
			System.out.println(s + ": " + hash.get(s).size());
			System.err.println(hash.get(s));
		}

	}

}
