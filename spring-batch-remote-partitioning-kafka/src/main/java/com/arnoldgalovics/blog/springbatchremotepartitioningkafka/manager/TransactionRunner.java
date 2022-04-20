package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import java.util.function.Supplier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionRunner {
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void runInTransaction(Supplier<Void> supp) {
        supp.get();
    }
}
