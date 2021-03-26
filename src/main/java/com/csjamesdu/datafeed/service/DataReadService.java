package com.csjamesdu.datafeed.service;

public interface DataReadService {

    void export();

    void export(String name, Integer attempts, Long timeout);
}
