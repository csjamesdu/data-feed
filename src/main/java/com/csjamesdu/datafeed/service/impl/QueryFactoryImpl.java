package com.csjamesdu.datafeed.service.impl;

import com.csjamesdu.datafeed.service.QueryFactory;
import org.springframework.stereotype.Service;

@Service
public class QueryFactoryImpl implements QueryFactory {

    private static final String DB_NAME = "sakila";

    @Override
    public String selectQueryByName(String name) {

//       return "SELECT * FROM " + DB_NAME + "." + name;
       return "SELECT * FROM " + name;

    }
}
