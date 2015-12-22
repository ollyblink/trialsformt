package mapreduce.manager.conditions;

import java.util.List;

import mapreduce.execution.job.Job;

public class JobEmptyCondition implements ICondition<List<Job>> {

	private JobEmptyCondition() {

	}

	public static JobEmptyCondition create() {
		return new JobEmptyCondition();
	}

	@Override
	public boolean metBy(List<Job> t) {
		return t.isEmpty();
	}
}