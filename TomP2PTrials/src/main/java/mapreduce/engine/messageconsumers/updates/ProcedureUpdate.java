package mapreduce.engine.messageconsumers.updates;

import mapreduce.engine.messageconsumers.JobCalculationMessageConsumer;
import mapreduce.execution.domains.IDomain;
import mapreduce.execution.domains.JobProcedureDomain;
import mapreduce.execution.jobs.Job;
import mapreduce.execution.procedures.EndProcedure;
import mapreduce.execution.procedures.Procedure;

public class ProcedureUpdate implements IUpdate {
	private Job job;
	private JobCalculationMessageConsumer msgConsumer;

	public ProcedureUpdate(Job job, JobCalculationMessageConsumer msgConsumer) {
		this.job = job;
		this.msgConsumer = msgConsumer;
	}

	@Override
	public Procedure executeUpdate(IDomain outputDomain, Procedure procedure) {
		JobProcedureDomain outputJPD = (JobProcedureDomain) outputDomain;
		procedure.addOutputDomain(outputJPD);
		if (procedure.isFinished()) {
			msgConsumer.cancelProcedureExecution(procedure);
			job.incrementProcedureIndex();
			job.currentProcedure().dataInputDomain(outputJPD);
			if (job.currentProcedure().executable().getClass().getSimpleName().equals(EndProcedure.class.getSimpleName())) {
				job.isFinished(true);
				msgConsumer.printResults(job);
			} else {
				procedure = job.currentProcedure();
			}
		}
		return procedure;
	}
}