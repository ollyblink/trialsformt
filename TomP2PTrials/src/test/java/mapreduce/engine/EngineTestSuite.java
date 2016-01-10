package mapreduce.engine;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.engine.executor.MRJobExecutionManagerTest;
import mapreduce.engine.executor.MRJobSubmissionManagerTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ MRJobExecutionManagerTest.class, MRJobSubmissionManagerTest.class })
public class EngineTestSuite {

}
