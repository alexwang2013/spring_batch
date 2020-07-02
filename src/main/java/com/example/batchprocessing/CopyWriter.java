package com.example.batchprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.postgresql.copy.CopyManager;
import org.springframework.batch.item.ItemWriter;

import org.springframework.beans.factory.InitializingBean;

public class CopyWriter<T> implements ItemWriter<T>,InitializingBean {
    protected static final Log logger = LogFactory.getLog(CopyWriter.class);
    private CopyManager cm;
    private String input_folder = "/Users/wang/study/spring_batch/src/main/resources/thread/";
	private String file_target="/Users/wang/study/spring_batch/src/main/resources/temp-output.csv";

    public void setCm(CopyManager cm) {
        this.cm = cm;
    }
    
    @Override
	public void write(final List<? extends T> items) throws Exception {
        logger.info("##########Copy Writer");
        mergefiles(this.file_target,this.input_folder);
		long rowsInserted = cm
					.copyIn("COPY people FROM STDIN (FORMAT csv)", 
						new BufferedReader(new FileReader(file_target))
						);
		System.out.printf("%d row(s) inserted%n", rowsInserted);
    }

    @Override
	public void afterPropertiesSet() {
		
    }
    
    private void mergefiles(String file_target, String input_folder) throws IOException {
        List<Path> files_paths = new ArrayList<Path>();
		File folder = new File(input_folder);
		File[] listOfFiles = folder.listFiles();

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				System.out.println("File " + listOfFiles[i].getPath());
				files_paths.add(Paths.get(listOfFiles[i].getPath()));
			} else if (listOfFiles[i].isDirectory()) {
				System.out.println("Directory " + listOfFiles[i].getPath());
			}
		}
		
		List<String> mergedLines = getMergedLines(files_paths);
		Path target = Paths.get(file_target);
        Files.write(target, mergedLines);
    }

    private List<String> getMergedLines(List<Path> paths) throws IOException {
		List<String> mergedLines = new ArrayList<> ();
		for (Path p : paths){
			List<String> lines = Files.readAllLines(p);
			if (!lines.isEmpty()) {
				mergedLines.addAll(lines);
			}
		}
		return mergedLines;
	}

}