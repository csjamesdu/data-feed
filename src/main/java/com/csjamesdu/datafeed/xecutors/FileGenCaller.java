package com.csjamesdu.datafeed.xecutors;

import com.csjamesdu.datafeed.service.impl.DataReadServiceImpl;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class FileGenCaller implements Callable {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileGenCaller.class);
    private String name;
    private String fileDir;
    private List<Map<String, Object>> result;

    public FileGenCaller(String _name, String _fileDir, List<Map<String, Object>> _result){
        this.name = _name;
        this.fileDir = _fileDir;
        this.result = _result;
    }

    @Override
    public String call() throws Exception {
        XSSFWorkbook workBook = new XSSFWorkbook();
        XSSFSheet sheet = workBook.createSheet(name);
        writeHeaderLine(sheet, result);
        writeData(result, sheet);
        generateExcel(workBook, fileDir);
        return "F";
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
            LOGGER.info("File Not Found Exception: " , e);
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOGGER.info("IO Exception: " , e);
            throw new RuntimeException(e);
        }
    }
}
