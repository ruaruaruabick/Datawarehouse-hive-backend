package com.datawarehouse.hive.entity;

public class Director {
    private String dId;
    private String Name;

    public String getName() {
        return Name;
    }

    public String getdId() {
        return dId;
    }

    public void setName(String name) {
        Name = name;
    }

    public void setdId(String dId) {
        this.dId = dId;
    }
}
