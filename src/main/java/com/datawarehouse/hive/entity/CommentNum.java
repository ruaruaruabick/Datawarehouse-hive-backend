package com.datawarehouse.hive.entity;

import java.util.Date;

public class CommentNum {
    private String mId;
    private Date rDate;
    private String type;
    private String name;
    private int num;

    public void setType(String type) {
        this.type = type;
    }

    public void setrDate(Date rDate) {
        this.rDate = rDate;
    }

    public void setmId(String mId) {
        this.mId = mId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getType() {
        return type;
    }

    public String getmId() {
        return mId;
    }

    public Date getrDate() {
        return rDate;
    }

    public String getName() {
        return name;
    }

    public int getNum() {
        return num;
    }
}
