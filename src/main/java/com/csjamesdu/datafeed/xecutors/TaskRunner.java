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

    @Autowired
    private DataReadService dataReadService;

    @Value("#{'${my.list.of.tables}'.split(',')}")
    private List<String> tables;


    @Scheduled(fixedDelay = 3000)
    public void runTask() {

        dataFeedTask();

    }

    private void dataFeedTask(){
        tables.forEach(name->taskProcess(name));
    }

    private void taskProcess(String queryName){

        LOGGER.info("*******************************************");
        LOGGER.info("Start exporting file "+ queryName + " {} ", dateFormat.format(new Date()));
        dataReadService.export(queryName);
        LOGGER.info("Finish exporting file " + queryName + " {} ", dateFormat.format(new Date()));
        LOGGER.info("*******************************************");

    }
}
