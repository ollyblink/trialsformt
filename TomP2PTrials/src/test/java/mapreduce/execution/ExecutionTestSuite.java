package mapreduce.execution;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.jobtask.JobTaskTestSuite;
import mapreduce.execution.task.scheduling.SchedulingTestSuite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ JobTaskTestSuite.class, SchedulingTestSuite.class})

public class ExecutionTestSuite {

}
