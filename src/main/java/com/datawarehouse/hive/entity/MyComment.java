package com.datawarehouse.hive.entity;

import java.sql.Date;

public class MyComment {
    private String userId;
    private String summary;
    private String helpfulness;
    private String text;
    private String movieId;
    private Date time;//？？？
    private int score;

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public void setHelpfulness(String helpfulness) {
        this.helpfulness = helpfulness;
    }

    public void setTime(Date time) {
        this.time = time;
    }

    public void setMovieId(String movieId) {
        this.movieId = movieId;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getHelpfulness() {
        return helpfulness;
    }

    public String getMovieId() {
        return movieId;
    }

    public int getScore() {
        return score;
    }

    public Date getTime() {
        return time;
    }

    public String getSummary() {
        return summary;
    }

    public String getText() {
        return text;
    }

    public String getUserId() {
        return userId;
    }
}
