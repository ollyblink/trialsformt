package mapreduce;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.EngineTestSuite;
import mapreduce.execution.ExecutionTestSuite;
import mapreduce.storage.StorageTestSuite;
@RunWith(Suite.class)
@Suite.SuiteClasses({
	ExecutionTestSuite.class,
	EngineTestSuite.class,
	StorageTestSuite.class
})
public class MapReduceTestSuite {
 
}
