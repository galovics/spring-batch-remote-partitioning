package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionRunner {
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void runInTransaction(Runnable r) {
        r.run();
    }
}
