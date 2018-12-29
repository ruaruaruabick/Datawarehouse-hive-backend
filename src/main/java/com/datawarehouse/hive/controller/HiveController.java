package com.datawarehouse.hive.controller;


import com.datawarehouse.hive.entity.Movie;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import javax.sql.DataSource;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/hive")
public class HiveController {
    private static final Logger logger = LoggerFactory.getLogger(HiveController.class);

    @Autowired
    @Qualifier("hiveJdbcDataSource")
    org.apache.tomcat.jdbc.pool.DataSource jdbcDataSource;

    @Autowired
    @Qualifier("hiveDruidDataSource")
    DataSource druidDataSource;

    @RequestMapping("/table/list")
    public List<String> listAllTables() throws SQLException {
        List<String> list = new ArrayList<>();
        // Statement statement = jdbcDataSource.getConnection().createStatement();
        Statement statement = druidDataSource.getConnection().createStatement();
        String sql = "show tables";
        logger.info("Running: " + sql);
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            list.add(res.getString(1));
        }
        return list;
    }
    //XX年有多少电影
    @RequestMapping("/selectbytime/movienumbyyear")
    public Long queryMovieNumByYear(@RequestParam(value = "year") String year){
        try {
            Long movieNum = 0L;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from movie";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                if(res.getString(2).substring(1,0).equals(year)){
                    movieNum++;
                }
            }
            return movieNum;
        }catch (Exception e){
            return null;
        }
    }

    //XX年XX月有多少电影
    @RequestMapping("/selectbytime/movienumbymonth")
    public Long queryMovieNumByMonth(@RequestParam(value = "year") String year,@RequestParam(value = "month") String month){
        try {
            Long movieNum = 0L;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from movie";
            String time = year + month;
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                if(res.getString(2).substring(1,0).equals(time)){
                    movieNum++;
                }
            }
            return movieNum;
        }catch (Exception e){
            return null;
        }
    }

    //XX电影有多少版本,返回所有版本名和版本数????????????????
    /*@RequestMapping("/selectbyname/numofversion")
    public List queryMovieNumByNameVersion(@RequestParam(value = "name") String name,){
        try {
            int numVersion = 0;
            List result = new ArrayList();
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select type from movie";
            String time = year + month;
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                if(res.getString(2).substring(1,0).equals(time)){
                    movieNum++;
                }
            }
            return movieNum;
        }catch (Exception e){
            return null;
        }
    }*/
    //XX导演有多少电影
    @RequestMapping("/selectbydirector/movienum")
    public int queryMovieNumByDirector(@RequestParam(value = "name") String name){
        try {
            int movieNum = 0;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select count(MID) from directing where AID = (select DID from director where name="+name+")";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                movieNum = res.getInt(1);
            }
            return movieNum;
        }catch (Exception e){
            return -1;
        }
    }

    //XX演员演了多少电影
    @RequestMapping("/selectbyactor/movienum")
    public int queryMovieNumByActor(@RequestParam(value = "name") String name){
        try {
            int movieNum = 0;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select count(MID) from acting where AID = (select AID from actor where name="+name+")";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                movieNum = res.getInt(1);
            }
            return movieNum;
        }catch (Exception e){
            return -1;
        }
    }

    //某种类型电影有多少
    @RequestMapping("/selectbytype/movienum")
    public Long queryMovieNumByType(@RequestParam(value = "type") String type){
        try {
            long movieNum = 0L;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select count(MID) from movie where type="+type+")";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                movieNum = res.getLong(1);
            }
            return movieNum;
        }catch (Exception e){
            return null;
        }
    }

    //查找电影
    @RequestMapping("/select/movie")
    public List queryMovie(@RequestParam(value = "name") String name, @RequestParam(value = "actorname") String actorName,
                           @RequestParam(value = "start") String start, @RequestParam(value = "end") String end,
                           @RequestParam(value = "catalog") String type){
        try {
            List<Movie> resultList = new ArrayList<Movie>();
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from movie where ";
            if(!name.equals("")){
                    sql = sql + "name=" + name;
            }
            if (!actorName.equals("")){
                sql = sql + " and mid in (select mid from acting where aid = " +
                        "( select aid from actor where name = " + actorName + "))";
            }
            if(!start.equals("")){
                sql = sql + " and rdate > " + start;
            }
            if(!end.equals("")){
                sql = sql + " and rdate < " + end;
            }
            if(!type.equals("")){
                sql = sql + "and type =" + type;
            }
            sql = sql + ';';
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                Movie movie = new Movie();
                movie.setmId(res.getString("mid"));
                movie.setName(res.getString("name"));
                movie.setrDate(res.getDate("rdate"));
                movie.setType(res.getString("type"));
                resultList.add(movie);
            }
            return resultList;
        }catch (Exception e){
            return null;
        }
    }


}
