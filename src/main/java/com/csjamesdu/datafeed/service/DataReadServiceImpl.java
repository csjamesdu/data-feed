package com.csjamesdu.datafeed.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.*;
import org.springframework.stereotype.Service;

@Service
public class DataReadServiceImpl implements DataReadService{

    private static final Logger LOGGER = LoggerFactory.getLogger(DataReadServiceImpl.class);

    private static final String FILE_SUFFIX = "Excel.xlsx";

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
    public void export(String name) {
        String queryString = queryFactory.selectQueryByName(name);
        LOGGER.info("Query: " + queryString);

        String fileDir = name + FILE_SUFFIX;
        List<Map<String, Object>> result = jdbcTemplate.queryForList(queryString);
        XSSFWorkbook workBook = new XSSFWorkbook();
        XSSFSheet sheet = workBook.createSheet(name);

        writeHeaderLine(sheet, result);
        writeData(result, sheet);
        generateExcel(workBook, fileDir);


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
            LOGGER.warn("Result Set is Empty! Cannot Initialize Header Row");
        }

    }

    private void generateExcel(XSSFWorkbook workBook, String fileDir) {
        try {
            FileOutputStream outputStream = new FileOutputStream(fileDir);
            workBook.write(outputStream);
            workBook.close();
        } catch (FileNotFoundException e) {
            LOGGER.warn("Exception: " + e);
        } catch (IOException e) {
            LOGGER.warn("Exception: " + e);
        }
    }
}
