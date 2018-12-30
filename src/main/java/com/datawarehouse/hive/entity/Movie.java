package com.datawarehouse.hive.entity;

public class Movie {
    private String name;
    private String rDate;
    private String type;
    private String mId;

    public String getName() {
        return name;
    }

    public String getrDate() {
        return rDate;
    }

    public String getmId() {
        return mId;
    }

    public String getType() {
        return type;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setmId(String mId) {
        this.mId = mId;
    }

    public void setrDate(String rDate) {
        this.rDate = rDate;
    }

    public void setType(String type) {
        this.type = type;
    }
}
