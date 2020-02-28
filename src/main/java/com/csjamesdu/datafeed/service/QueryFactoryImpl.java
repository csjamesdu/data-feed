package com.csjamesdu.datafeed.service;

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
