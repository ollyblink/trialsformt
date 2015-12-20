package mapreduce.execution.jobtask;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import mapreduce.execution.task.TaskTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ 
	JobTest.class, 
	TaskTest.class
	})
public class JobTaskTestSuite {

}