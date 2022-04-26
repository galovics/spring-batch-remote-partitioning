package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import static com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager.ManagerConfiguration.PARTITIONER_STEP_NAME;
import static com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager.ManagerConfiguration.PARTITIONING_JOB_NAME;
import static org.springframework.batch.core.BatchStatus.COMPLETED;
import static org.springframework.batch.core.BatchStatus.FAILED;
import static org.springframework.batch.core.BatchStatus.STARTED;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.JobLocator;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Profile("manager")
public class ManagerStarter implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(ManagerStarter.class);

    @Autowired
    private JobOperator jobOperator;

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private JobLocator jobLocator;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionRunner txRunner;

    @Override
    public void run(String... args) throws Exception {
        if (areThereStuckJobs()) {
            List<Long> stuckJobIds = getStuckJobIds();
            stuckJobIds.forEach(this::handleStuckJob);
        } else {
            Job partitioningJob = jobLocator.getJob(PARTITIONING_JOB_NAME);
            JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                    .getNextJobParameters(partitioningJob)
                    .toJobParameters();
            jobLauncher.run(partitioningJob, jobParameters);
        }
    }

    private void handleStuckJob(Long stuckJobId) {
        try {
            waitUntilAllPartitionsFinished(stuckJobId);
            txRunner.runInTransaction(() -> {
                jdbcTemplate.update("UPDATE BATCH_STEP_EXECUTION SET STATUS = :status WHERE JOB_EXECUTION_ID = :jobExecutionId AND STEP_NAME = :stepName", Map.of(
                        "status", FAILED.name(),
                        "jobExecutionId", stuckJobId,
                        "stepName", PARTITIONER_STEP_NAME));
                jdbcTemplate.update("UPDATE BATCH_JOB_EXECUTION SET STATUS = :status, START_TIME = null, END_TIME = null WHERE JOB_EXECUTION_ID = :jobExecutionId", Map.of(
                        "status", FAILED.name(),
                        "jobExecutionId", stuckJobId));
            });
            jobOperator.restart(stuckJobId);
        } catch (Exception e) {
            throw new RuntimeException("Exception while handling a stuck job", e);
        }
    }

    private void waitUntilAllPartitionsFinished(Long stuckJobId) throws InterruptedException {
        while (!areAllPartitionsCompleted(stuckJobId)) {
            logger.info("Sleeping for a second to wait for the partitions to complete for job {}", stuckJobId);
            Thread.sleep(1000);
        }
    }

    private boolean areAllPartitionsCompleted(Long jobId) {
        Long partitionsNotCompleted = jdbcTemplate.queryForObject("SELECT COUNT(bse.STEP_EXECUTION_ID) FROM BATCH_STEP_EXECUTION bse " +
                "WHERE bse.JOB_EXECUTION_ID = :jobExecutionId AND bse.STEP_NAME <> :stepName AND bse.status <> :status", Map.of(
                "jobExecutionId", jobId,
                "stepName", PARTITIONER_STEP_NAME,
                "status", COMPLETED.name()), Long.class);
        return partitionsNotCompleted == 0L;
    }

    private List<Long> getStuckJobIds() {
        return jdbcTemplate.queryForList("SELECT bje.JOB_EXECUTION_ID FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS IN (:statuses) AND bji.JOB_NAME = :jobName AND bje.JOB_INSTANCE_ID NOT IN (" +
                "SELECT bje.JOB_INSTANCE_ID FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS = :completedStatus AND bji.JOB_NAME = :jobName)", Map.of(
                "statuses", List.of(STARTED.name(), FAILED.name()),
                "jobName", PARTITIONING_JOB_NAME,
                "completedStatus", COMPLETED.name()), Long.class);
    }

    private boolean areThereStuckJobs() {
        Long stuckJobCount = jdbcTemplate.queryForObject("SELECT COUNT(*) as STUCK_JOB_COUNT FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS IN (:statuses) AND bji.JOB_NAME = :jobName AND bje.JOB_INSTANCE_ID NOT IN (" +
                "SELECT bje.JOB_INSTANCE_ID FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS  = :completedStatus AND bji.JOB_NAME = :jobName)", Map.of(
                "statuses", List.of(STARTED.name(), FAILED.name()),
                "jobName", PARTITIONING_JOB_NAME,
                "completedStatus", COMPLETED.name()), Long.class);
        return stuckJobCount != 0L;
    }
}
