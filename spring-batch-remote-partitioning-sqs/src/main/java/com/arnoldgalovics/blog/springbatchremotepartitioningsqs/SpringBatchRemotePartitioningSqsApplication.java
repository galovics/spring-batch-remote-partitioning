package com.arnoldgalovics.blog.springbatchremotepartitioningsqs;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableBatchProcessing
@EnableBatchIntegration
@SpringBootApplication
public class SpringBatchRemotePartitioningSqsApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBatchRemotePartitioningSqsApplication.class, args);
	}

}
