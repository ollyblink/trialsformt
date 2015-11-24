package mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.ExecutionTestSuite;
import mapreduce.server.ServerTestSuite;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	ExecutionTestSuite.class,
	ServerTestSuite.class
})
public class MapReduceTestSuite {
 
}
