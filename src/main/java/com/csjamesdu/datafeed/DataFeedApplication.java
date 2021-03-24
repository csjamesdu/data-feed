package com.csjamesdu.datafeed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataFeedApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(DataFeedApplication.class);

	public static void main(String[] args) {


		SpringApplication.run(DataFeedApplication.class, args);

	}

}
