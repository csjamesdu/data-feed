package com.csjamesdu.datafeed.xecutors;

import com.csjamesdu.datafeed.service.DataReadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Service
public class TaskRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskRunner.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    private static final int ATTEMPTS = 10;
    private static final long TIMEOUT = 15000;
    private static final long STEP = 3000;

    @Autowired
    private DataReadService dataReadService;

    @Value("#{'${my.list.of.tables}'.split(',')}")
    private List<String> tables;


    @Scheduled(fixedDelay = STEP)
    public void runTask() {

        dataFeedTask();

    }

    private void dataFeedTask(){
        tables.forEach(name->taskProcess(name, ATTEMPTS, TIMEOUT));
    }

    private void taskProcess(String queryName, Integer attempts, Long timeout){

        LOGGER.info("*******************************************");
        LOGGER.info("Start exporting file "+ queryName + " {} ", dateFormat.format(new Date()));
        dataReadService.export(queryName, attempts, timeout);
        LOGGER.info("Finish exporting file " + queryName + " {} ", dateFormat.format(new Date()));
        LOGGER.info("*******************************************");

    }
}
