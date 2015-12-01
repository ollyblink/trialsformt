package mapreduce.execution.taskresultcomparison;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import mapreduce.execution.taskresultcomparison.HashTaskResultComparator;
import mapreduce.execution.taskresultcomparison.ITaskResultComparator;

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

		List<Integer> results = new ArrayList<Integer>();

		results.add(6+2+3+2);
		results.add(13);
		results.add(10+1+1+1);
		results.add(4+4+4+1); 
		
//		comparator.evaluateFinalResult();

		Multimap<Integer, Integer> hash = ArrayListMultimap.create();
		for (Integer s : results) {
			hash.put(s, 1);
		}

		for (Integer s : hash.keySet()) {
			System.out.println(s + ": " + hash.get(s).size());
			System.err.println(hash.get(s));
		}

	}

}
