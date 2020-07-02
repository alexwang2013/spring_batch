package com.example.batchprocessing;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.ItemWriter;

import org.springframework.beans.factory.InitializingBean;

public class CsvWriter<T> implements ItemWriter<T>,InitializingBean {
    protected static final Log logger = LogFactory.getLog(CsvWriter.class);
    @Override
	public void write(final List<? extends T> items) throws Exception {
        logger.debug("##########CsvWriter");
        Thread currentThread = Thread.currentThread();
        logger.info("id of the thread is " + currentThread.getId());
        String directoryName = "/Users/wang/study/spring_batch/src/main/resources/thread";
        File directory = new File(directoryName);
        if (! directory.exists()){
            directory.mkdir();
            // If you require it to make the entire directory path including parents,
            // use directory.mkdirs(); here instead.
        }
        String filePath=directoryName+"/"+currentThread.getId()+".csv";
        FileWriter writer = new FileWriter(filePath,true);
        for (T p : items) {
            
            List<String> list = new ArrayList<>();
            list.add(((Person)p).getFirstName());
            list.add(((Person)p).getLastName());
            CSVUtils.writeLine(writer, list);

        }
        writer.flush();
        writer.close();

    }

    @Override
	public void afterPropertiesSet() {
		
	}
}