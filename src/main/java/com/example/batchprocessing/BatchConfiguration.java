package com.example.batchprocessing;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

// tag::setup[]
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {

	private static final Logger log = LoggerFactory.getLogger(BatchConfiguration.class);

	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;
	// end::setup[]

	@Bean
	public CustomMultiResourcePartitioner partitioner() {
		CustomMultiResourcePartitioner partitioner 
		= new CustomMultiResourcePartitioner();
		return partitioner;
	}
	// tag::readerwriterprocessor[]
	@Bean
	@StepScope
	public FlatFileItemReader<Person> reader() {
		return new FlatFileItemReaderBuilder<Person>().name("personItemReader")
				.resource(new ClassPathResource("sample-data.csv")).delimited()
				.names(new String[] { "firstName", "lastName" })
				.fieldSetMapper(new BeanWrapperFieldSetMapper<Person>() {
					{
						setTargetType(Person.class);
					}
				}).build();
	}

	@Bean
	public ListItemReader<String> reader2() throws IOException {
		String empty_ele="";
		List<String> list = new ArrayList<String>();
		list.add(empty_ele);
		return new ListItemReader<>(list);
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}

	@Bean
	public NoProcesser processor2() {
		return new NoProcesser();
	}

	@Bean
	@StepScope
	public CsvWriter<Person> writer(DataSource dataSource) {
		CsvWriter<Person> itemWriter = new CsvWriter<>();

		return itemWriter;
	}

	@Bean
	public CopyWriter<String> writer2(DataSource dataSource)
			throws SQLException, FileNotFoundException, IOException {
		log.info("!writer2! xyz");

		CopyWriter<String> itemWriter = new CopyWriter<>();
		// itemWriter.setSql("INSERT INTO people (first_name, last_name) VALUES (:firstName, :lastName)");
		// itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<Person>());
		PGConnection pgConnection = null;
		try (Connection conn = dataSource.getConnection()) {
			if (conn.isWrapperFor(PGConnection.class)) {
				pgConnection = conn.unwrap(PGConnection.class);
			}
			CopyManager cm = new CopyManager((BaseConnection) pgConnection);
			itemWriter.setCm(cm);
		}
		return itemWriter;
	}
	// end::readerwriterprocessor[]

	// tag::jobstep[]
	@Bean
	public Job importUserJob(JobCompletionNotificationListener listener, Step partitionStep1, Step step2) {
		return jobBuilderFactory.get("importUserJob")
			.incrementer(new RunIdIncrementer())
			.listener(listener)
			.flow(partitionStep1).next(step2)
			.end()
			.build();
	}

	@Bean
	public Step partitionStep1(CsvWriter<Person> writer) 
	throws UnexpectedInputException, MalformedURLException, ParseException {
		return stepBuilderFactory.get("partitionStep1")
		.partitioner(step1(writer).getName(), partitioner())
		.step(step1(writer))
		.gridSize(3)
		.taskExecutor(new SimpleAsyncTaskExecutor())
		.build();
	}

	@Bean
	public Step step1(CsvWriter<Person> writer) {
		return stepBuilderFactory.get("step1")
			.<Person, Person> chunk(2)
			.reader(reader())
			.processor(processor())
			.writer(writer)
			.build();
	}
	@Bean
	public Step step2(CopyWriter<String> writer2) throws IOException {
		return stepBuilderFactory.get("step2")
			.<String, String> chunk(2)
			.reader(reader2())
			.processor(processor2())
			.writer(writer2)
			.build();
	}
	// end::jobstep[]
}