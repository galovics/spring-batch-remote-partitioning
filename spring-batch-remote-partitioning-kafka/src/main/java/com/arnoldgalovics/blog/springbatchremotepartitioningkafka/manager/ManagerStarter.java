package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

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
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate txTemplate;

    @Override
    public void run(String... args) throws Exception {
        if (isThereStuckJobs()) {
            List<Long> stuckJobIds = getStuckJobIds();
            stuckJobIds.forEach(this::handleStuckJob);
        } else {
            Job partitioningJob = jobLocator.getJob("partitioningJob");
            JobParameters jobParameters = new JobParametersBuilder(jobExplorer)
                    .getNextJobParameters(partitioningJob)
                    .toJobParameters();
            jobLauncher.run(partitioningJob, jobParameters);
        }
    }

    private void handleStuckJob(Long stuckJobId) {
        try {
            waitUntilAllPartitionsFinished(stuckJobId);
            txTemplate.execute((status) -> {
                jdbcTemplate.update("UPDATE BATCH_STEP_EXECUTION SET STATUS = 'COMPLETED' WHERE JOB_EXECUTION_ID = " + stuckJobId + " AND STEP_NAME = 'partitionerStep'");
                jdbcTemplate.update("UPDATE BATCH_JOB_EXECUTION SET STATUS = 'FAILED' WHERE JOB_EXECUTION_ID = " + stuckJobId);
                return null;
            });
            jobOperator.restart(stuckJobId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void waitUntilAllPartitionsFinished(Long stuckJobId) throws InterruptedException {
        while(!areAllPartitionsCompleted(stuckJobId)) {
            logger.info("Sleeping for a second to wait for the partitions to complete for job {}", stuckJobId);
            Thread.sleep(1000);
        }
    }

    private boolean areAllPartitionsCompleted(Long jobId) {
        Long partitionsNotCompleted = jdbcTemplate.queryForObject("SELECT COUNT(bse.STEP_EXECUTION_ID) FROM BATCH_STEP_EXECUTION bse " +
                "WHERE bse.JOB_EXECUTION_ID = " + jobId + " AND bse.STEP_NAME <> 'partitionerStep' AND bse.status <> 'COMPLETED'", Long.class);
        return partitionsNotCompleted == 0L;
    }

    private List<Long> getStuckJobIds() {
        return jdbcTemplate.query("SELECT * FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS = 'STARTED' AND bji.JOB_NAME = 'partitioningJob'", (rs, rowNum) -> {
                    return rs.getLong("JOB_EXECUTION_ID");
                });
    }

    private boolean isThereStuckJobs() {
        Long stuckJobCount = jdbcTemplate.queryForObject("SELECT COUNT(*) as STUCK_JOB_COUNT FROM BATCH_JOB_INSTANCE bji " +
                "INNER JOIN BATCH_JOB_EXECUTION bje ON bji.JOB_INSTANCE_ID = bje.JOB_INSTANCE_ID " +
                "WHERE bje.STATUS = 'STARTED' AND bji.JOB_NAME = 'partitioningJob'", Long.class);
        return stuckJobCount != 0L;
    }

}
