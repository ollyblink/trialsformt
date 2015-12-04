package mapreduce.manager.conditions;

import java.util.Set;

import mapreduce.manager.broadcasthandler.broadcastmessages.BCMessageStatus;
import mapreduce.manager.broadcasthandler.broadcastmessages.IBCMessage;
import mapreduce.manager.broadcasthandler.broadcastmessages.IJobBCMessage;

public final class JobBCMessageUpdateCondition implements ICondition<IBCMessage> {

	private String jobId;
	private Set<BCMessageStatus> types;

	@Override
	public boolean metBy(IBCMessage t) {
		if (t != null && t instanceof IJobBCMessage) {
			IJobBCMessage jobBCMessage = (IJobBCMessage) t;
			return (!jobBCMessage.jobId().equals(jobId) && !types.contains(jobBCMessage.status()));
		} else {
			return false;
		}
	}

	private JobBCMessageUpdateCondition() {
		this.jobId = null;
		this.types = null;
	}

	public static JobBCMessageUpdateCondition newInstance() {
		return new JobBCMessageUpdateCondition();
	}

	public JobBCMessageUpdateCondition jobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public JobBCMessageUpdateCondition types(Set<BCMessageStatus> types) {
		this.types = types;
		return this;
	}
}
