package com.csjamesdu.datafeed.service.impl;

import com.csjamesdu.datafeed.xecutors.MyRetryTemplate;
import com.csjamesdu.datafeed.service.DataReadService;
import com.csjamesdu.datafeed.service.QueryFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

@Service
public class DataReadServiceImpl implements DataReadService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataReadServiceImpl.class);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    private static final String FILE_SUFFIX = "_Excel.xlsx";
    private static final String FILE_DIR = "C:\\ForgeHardware\\ERP\\DataBases\\";

    @Value("retry.attempts")
    private String retryAttepts;

    @Value("retry.step")
    private String retryStep;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private QueryFactory queryFactory;

    @Override
    public void export() {

        List<Map<String, Object>> result = jdbcTemplate.queryForList("SELECT * FROM sakila.actor;");
        XSSFWorkbook workBook = new XSSFWorkbook();
        XSSFSheet sheet = workBook.createSheet("Actor");

        writeHeaderLine(sheet, result);

        writeData(result, sheet);

        generateExcel(workBook, "testExcel.xlsx");


    }

    @Override
    public void export(String name, Integer attempts, Long timeout) {

        String queryString = queryFactory.selectQueryByName(name);
        LOGGER.info("Query: " + queryString);

        String fileDir = FILE_DIR + name + FILE_SUFFIX;

        LOGGER.info(">>>>>>DB Query Starts at: " + dateFormat.format(new Date()));
//        List<Map<String, Object>> result = jdbcTemplate.queryForList(queryString);

        List<Map<String, Object>> result = null;
        try {
//            result = queryDBWithSimpleRetry(queryString);
            result = queryDBWithTimeOutRetryPolicy(queryString, attempts, timeout);
        } catch (Exception e) {
            LOGGER.info("DataBase query failed, please wait for the next execution");
            LOGGER.info("", e);
        }
        LOGGER.info(">>>>>>DB Query Ends at: " + dateFormat.format(new Date()));

        if (result != null) {
            LOGGER.info("<<<<<<File Generation Starts at: " + dateFormat.format(new Date()));
            fileGen(name, result, fileDir);
            LOGGER.info("<<<<<<File Generation Ends at: " + dateFormat.format(new Date()));
        }

    }

    private void fileGen(String name, List<Map<String, Object>> result, String fileDir) {
        XSSFWorkbook workBook = new XSSFWorkbook();
        XSSFSheet sheet = workBook.createSheet(name);
        writeHeaderLine(sheet, result);
        writeData(result, sheet);
        generateExcel(workBook, fileDir);
    }

    private List<Map<String, Object>> queryDBWithSimpleRetry(String queryString) throws Exception{
        RetryTemplate template = new RetryTemplate();
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(10);
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(6000);
        template.setRetryPolicy(retryPolicy);
        template.setBackOffPolicy(backOffPolicy);

        List<Map<String, Object>> result = template.execute(new RetryCallback<List<Map<String, Object>>, Exception>() {
            @Override
            public List<Map<String, Object>> doWithRetry(RetryContext retryContext) throws Exception {
                List<Map<String, Object>> queryResult = null;
                try{
                    queryResult = jdbcTemplate.queryForList(queryString);

                } catch (DataAccessException e){
//                    LOGGER.info(e.toString());
                    LOGGER.info("Retry Count: " +
                            new Integer(retryContext.getRetryCount() + 1).toString() +
                            " Starts at : " +
                            dateFormat.format(new Date()));
                    throw new RuntimeException();
                }
                return queryResult;
            }
        });

        return result;
    }

    private List<Map<String, Object>> queryDBWithTimeOutRetryPolicy(String queryString, Integer attempts, Long timeout) throws Exception{

        final List<Map<String, Object>> resultList = new MyRetryTemplate<List<Map<String, Object>>>(attempts, timeout).execute(() -> {
            // retryable data query
            return jdbcTemplate.queryForList(queryString);
        });

        return resultList;
    }

    private void writeData(List<Map<String, Object>> result,  XSSFSheet sheet) {

        for(int i=1; i<=result.size(); i++){
            Row dataRow = sheet.createRow(i);
            Cell dataCell;
            Map<String, Object> dataEntry = result.get(i-1);
            for(int j=0; j<dataEntry.keySet().size(); j++) {
                dataCell = dataRow.createCell(j);
                Object val = dataEntry.values().toArray()[j];
                if (val != null) {
                    dataCell.setCellValue(dataEntry.values().toArray()[j].toString());
                } else {
                    dataCell.setCellValue("");
                }
            }
        }

    }

    private void writeHeaderLine(XSSFSheet sheet, List<Map<String, Object>> result) {

        Row headerRow = sheet.createRow(0);
        Cell headerCell;

        Map<String, Object> sampleResult = result.get(0);
        if(sampleResult != null) {
            for (int i = 0; i < sampleResult.keySet().size(); i++) {
                headerCell = headerRow.createCell(i);
                headerCell.setCellValue(sampleResult.keySet().toArray()[i].toString());
            }
        } else {
            LOGGER.info("Result Set is Empty! Cannot Initialize Header Row");
        }

    }

    private void generateExcel(XSSFWorkbook workBook, String fileDir) {
        try(FileOutputStream outputStream = new FileOutputStream(fileDir)) {
            workBook.write(outputStream);
            workBook.close();
        } catch (FileNotFoundException e) {
            LOGGER.info("Exception: " , e);
        } catch (IOException e) {
            LOGGER.info("Exception: " , e);
        }
    }
}
