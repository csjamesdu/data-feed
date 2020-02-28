package com.csjamesdu.datafeed.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static java.lang.Thread.sleep;

@Service
public class TaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    @Autowired
    private DataReadService dataReadService;

    @Value("#{'${my.list.of.tables}'.split(',')}")
    private List<String> tables;

    @Scheduled(fixedRate = 10000, initialDelay = 1000)
    public void runTask() {

        tables.forEach(name->taskProcess(name));

    }

    private void taskProcess(String queryName){
        try {
            LOGGER.info("*******************************************");
            LOGGER.info("Start exporting file "+ queryName + " {} ", dateFormat.format(new Date()));
            dataReadService.export(queryName);
            LOGGER.info("Finish exporting file " + queryName + " {} ", dateFormat.format(new Date()));
            LOGGER.info("*******************************************");
            sleep(1000);
        } catch (InterruptedException e) {
            LOGGER.warn("Thread sleep is interrupted: " + e);
        }

    }
}
