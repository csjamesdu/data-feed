package com.csjamesdu.datafeed.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

@Service
public class TaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Autowired
    private DataReadService dataReadService;

    @Scheduled(fixedRate = 10000, initialDelay = 1000)
    public void runTask() {

        List<String> tableNames = new ArrayList<>();
        tableNames.add("actor");
        tableNames.add("address");
        tableNames.add("city");
        tableNames.add("category");
        tableNames.add("country");

        tableNames.forEach(name->taskProcess(name));

    }

    private void taskProcess(String queryName){
        LOGGER.info("Start exporting file "+ queryName + " {} ", dateFormat.format(new Date()));
        dataReadService.export(queryName);
        try {
            sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread sleep is interrupted: " + e);
        }
        LOGGER.info("Finish exporting file " + queryName + " {} ", dateFormat.format(new Date()));

    }
}
