package com.example.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.batch.item.ItemProcessor;

public class NoProcesser implements ItemProcessor<String, String> {

  private static final Logger log = LoggerFactory.getLogger(NoProcesser.class);

  @Override
  public String process(String filepath) throws Exception {
    return filepath;
  }
}