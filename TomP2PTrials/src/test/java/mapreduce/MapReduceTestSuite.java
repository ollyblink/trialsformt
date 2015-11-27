package mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.ExecutionTestSuite;
import mapreduce.server.ServerTestSuite;
import mapreduce.storage.StorageTestSuite;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	ExecutionTestSuite.class,
	ServerTestSuite.class,
	StorageTestSuite.class
})
public class MapReduceTestSuite {
 
}
