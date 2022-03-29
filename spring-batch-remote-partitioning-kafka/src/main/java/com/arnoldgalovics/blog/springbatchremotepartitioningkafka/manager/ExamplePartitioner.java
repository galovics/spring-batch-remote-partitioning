package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.manager;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class ExamplePartitioner implements Partitioner {
    public static final String PARTITION_PREFIX = "partition";

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int partitionCount = 50;
        Map<String, ExecutionContext> partitions = new HashMap<>();
        for (int i = 0; i < partitionCount; i++) {
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.put("data", new ArrayList<Integer>());
            partitions.put(PARTITION_PREFIX + i, executionContext);
        }
        for (int i = 0; i < 1000; i++) {
            String key = PARTITION_PREFIX + (i % partitionCount);
            ExecutionContext executionContext = partitions.get(key);
            List<Integer> data = (List<Integer>) executionContext.get("data");
            data.add(i + 1);
        }
        return partitions;
    }
}
