package com.example.batchprocessing;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

public class CustomMultiResourcePartitioner implements Partitioner {
  
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        Map<String, ExecutionContext> map = new HashMap<>(gridSize);
        int i = 0, k = 1;
        for (i=0; i<=gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            
            map.put("Partition" + i, context);
            i++;
        }
        return map;
    }
}