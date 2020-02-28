package com.csjamesdu.datafeed;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class DataFeedApplication {

	public static void main(String[] args) {

		SpringApplication.run(DataFeedApplication.class);
	}

}
