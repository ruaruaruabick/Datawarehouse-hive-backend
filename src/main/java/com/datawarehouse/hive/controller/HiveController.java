package com.datawarehouse.hive.controller;


import com.datawarehouse.hive.entity.*;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
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
import java.util.*;

@RestController
@RequestMapping("/hive")
public class HiveController {
    private static final Logger logger = LoggerFactory.getLogger(HiveController.class);

    @Autowired
    @Qualifier("hiveDruidTemplate")
    private JdbcTemplate jdbcTemplate;

    //检查输入的类型是否正确
    private final String checkTypeSql = "select count(*) from comm_num where type = ?";
    //获取某类型电影每天的排片数
    private final String dateSql = "select count(*) as dateNum,avg (num) as avgNum ,fdate from" +
            "(select to_char(rdate,'mm-dd') as fdate , num from comm_num where type = ?)"+
            "group by fdate order by fdate";
    //获取某类型电影每天的平均票房
    private  final String dateComSql = "select avg (num) as dateNum,fdate from" +
            "(select to_char(rdate,'mm-dd') as fdate , num from comm_num where type = ?)"+
            "group by fdate order by fdate";
    //获取导演ID
    private final String directorNameSql = "select * from director where name = ?";
    //获取演员ID
    private  final String actorNameSql = "select * from actor where name = ?";
    //获取导演详细信息
    private final String directorSql =  "select * from director where did = ? ";
    //获取导演所有的电影信息
    private final String directingSql = "select * from directing where did = ? ";
    //获取演员详细信息
    private final String actorSql = "select * from actor where aid = ? ";
    //获取演员所有的参演电影信息
    private final String actingSql = "select * from acting where aid = ? ";
    //获取电影详细信息
    private final String movieSql =  "select * from movie where mid = ? ";
    //获取合作演员信息
    private final String coactingSql = "select * from acting where mid = ? ";
    //获取合作导演信息
    private final String codirectingSql = "select * from directing where mid = ? ";

    RowMapper<Acting> actingMapper = new BeanPropertyRowMapper<>(Acting.class);
    RowMapper<Actor> actorMapper = new BeanPropertyRowMapper<>(Actor.class);
    RowMapper<Director> directorMapper = new BeanPropertyRowMapper<>(Director.class);
    RowMapper<Directing> directingMapper = new BeanPropertyRowMapper<>(Directing.class);
    RowMapper<Movie> movieMapper = new BeanPropertyRowMapper<>(Movie.class);
    RowMapper<CommentNum> commMapper = new BeanPropertyRowMapper<>(CommentNum.class);

    @RequestMapping("/actor")
    @ResponseBody
    public Map actor(@RequestParam(value = "name") String name){
        Map res = new HashMap();

        Actor actor;

        //查找演员自身信息
        try {
            //throw new Exception("Nothing");
            actor = jdbcTemplate.queryForObject(actorNameSql, actorMapper, name);
            String id = actor.getaId();
            //查找演员出演电影信息
            List<Acting> actings = jdbcTemplate.query(actingSql, actingMapper, id);

            //存电影详细信息
            List movieDetail = new ArrayList();
            //存合作演员和电影的信息
            Map actor2Movie = new HashMap();

            for (Acting acting : actings) {
                //获取电影ID
                String mid = acting.getmId();
                Movie movie = null;
                //获取电影详细信息
                try{
                    movie = jdbcTemplate.queryForObject(movieSql, movieMapper, mid);
                }
                catch(Exception e){
                    System.out.println("没有找到电影");
                }
                if(movie != null){
                    movieDetail.add(movie);
                }


                //获取合作演员信息
                List<Acting> coActors = jdbcTemplate.query(coactingSql, actingMapper, mid);
                for(Acting coActor : coActors){
                    String aid = coActor.getaId();
                    //除去查找演员自身的ID
                    if(aid.equals(id)){
                        continue;
                    }
                    findCoPartner(actor2Movie, mid, aid,true,true);
                }
            }

            //存合作导演和电影的信息
            Map director2Movie = new HashMap();
            for (Acting acting : actings) {
                //获取电影ID
                String mid = acting.getmId();
                List<Directing> coDirectors = jdbcTemplate.query(codirectingSql, directingMapper, mid);
                //获取合作导演信息
                for(Directing coDirector : coDirectors){
                    String did = coDirector.getdId();
                    findCoPartner(director2Movie, mid, did,false,true);
                }
            }
            setRes(res, movieDetail, actor2Movie, director2Movie, name);
            return res;
        } catch (Exception e) {
            System.out.println("捕获到异常。。");
            res.put("err",-1);
            return  res;
        }
    }

    @RequestMapping("/director")
    @ResponseBody
    public Map director(@RequestParam(value = "name") String name){
        Map res = new HashMap();

        Director director;

        //查找导演自身信息
        try {
            //throw new Exception("Nothing");
            director = jdbcTemplate.queryForObject(directorNameSql, directorMapper, name);

            String id = director.getdId();
            //查找导演电影信息
            List<Directing> directings = jdbcTemplate.query(directingSql, directingMapper, id);

            //存电影详细信息
            List movieDetail = new ArrayList();
            //存合作演员和电影的信息
            Map actor2Movie = new HashMap();

            for (Directing directing : directings) {
                //获取电影ID
                String mid = directing.getmId();
                Movie movie = null;
                //获取电影详细信息
                try{
                    movie = jdbcTemplate.queryForObject(movieSql, movieMapper, mid);
                }
                catch(Exception e){
                    System.out.println("没有找到电影");
                }
                if(movie != null){
                    movieDetail.add(movie);
                }

                //获取合作演员信息
                List<Acting> coActors = jdbcTemplate.query(coactingSql, actingMapper, mid);
                for(Acting coActor : coActors){
                    String aid = coActor.getaId();
                    findCoPartner(actor2Movie, mid, aid,true,true);
                }
            }

            //存合作导演和电影的信息
            Map director2Movie = new HashMap();
            for (Directing directing : directings) {
                //获取电影ID
                String mid = directing.getmId();
                List<Directing> coDirectors = jdbcTemplate.query(codirectingSql, directingMapper, mid);
                //获取合作导演信息
                for(Directing coDirector : coDirectors){
                    String did = coDirector.getdId();
                    //除去查找导演自身的ID
                    if(did.equals(id)){
                        continue;
                    }
                    findCoPartner(director2Movie, mid, did,false,true);
                }
            }
            setRes(res, movieDetail, actor2Movie, director2Movie, name);
            return res;
        } catch (Exception e) {
            System.out.println("捕获到异常。。");
            res.put("err",-1);
            return  res;
        }
    }

    @RequestMapping("/date")
    @ResponseBody
    public List date(@RequestParam(value = "type") String type){
        //检查输入
        long count = jdbcTemplate.queryForObject(checkTypeSql, long.class, type);
        if(count <= 0){
            System.out.println("捕获到异常。。");
            Map res = new HashMap();
            res.put("err",-1);
            return Arrays.asList(res);
        }
        //获取平均票房
//        List<Map<String,Object>> avgCom = jdbcTemplate1.queryForList(dateComSql,type);
        //获取排片数
        List<Map<String,Object>> num = jdbcTemplate.queryForList(dateSql,type);
        return num;
    }

    //查找电影
    @RequestMapping("/oracle/movie")
    public List queryMovie(@RequestParam(value = "name") String name, @RequestParam(value = "actorname") String actorName,
                           @RequestParam(value = "start") String start, @RequestParam(value = "directorname") String directorName,
                           @RequestParam(value = "end") String end, @RequestParam(value = "catalog") String type){
        try {

            String sql = getSql(name, actorName,directorName, start, end, type);
//            ResultSet res = statement.executeQuery(sql);
            List<Movie> movies = jdbcTemplate.query(sql,movieMapper);

            return movies;
        }catch (Exception e){
            System.out.println("出错");
            e.printStackTrace();
            return null;
        }
    }


    //查找合作伙伴
    private void findCoPartner(Map partner2Movie, String mid, String aid, boolean isActor,boolean isOracle) {
        //如果已经有该演员或导演合作记录，修改原记录
        if(partner2Movie.containsKey(aid)){
            Map details = (HashMap)partner2Movie.get(aid);
            List<String> movies = (ArrayList<String>) details.get("movies");
            movies.add(mid);
            details.put("movies",movies);
            partner2Movie.put(aid,details);
        }
        //如果没有合作记录，新添加一份
        else{
            Map partnerDetail = new HashMap();
            if(isActor){
                Actor partner = jdbcTemplate.queryForObject(actorSql, actorMapper, aid);
                partnerDetail.put("name",partner.getName());
            }
            else{
                Director partner = jdbcTemplate.queryForObject(directorSql, directorMapper, aid);
                partnerDetail.put("name",partner.getName());
            }
            partnerDetail.put("movies",new ArrayList<>(Arrays.asList(mid)));
            partner2Movie.put(aid,partnerDetail);
        }
    }

    private void setRes(Map res, List movieDetail, Map actor2Movie, Map director2Movie, String name) {
        System.out.println("执行完成");

        res.put("err",0);
        res.put("name", name);
        res.put("actors", actor2Movie);
        res.put("directors", director2Movie);
        res.put("movies", movieDetail);
    }

    private String getSql(@RequestParam("name") String name, @RequestParam("actorname") String actorName,
                          @RequestParam("start") String start,@RequestParam(value = "directorname") String directorName,
                          @RequestParam("end") String end, @RequestParam("catalog") String type) {
        int count = 0;
//            Statement statement = druidDataSource.getConnection().createStatement();
        String sql = "select * from movie";
        if (!name.equals("")) {
            sql = sql + " where name = '" + name + "'";
            count += 1;
        }
        if (!actorName.equals("")) {
            if (count != 0) {
                sql = sql + " and mid in (select mid from acting where aid = " +
                        "( select aid from actor where name = '" + actorName + "'))";
            } else {
                sql = sql + " where mid in (select mid from acting where aid = " +
                        "( select aid from actor where name = '" + actorName + "'))";
                count += 1;
            }
        }
        if (!directorName.equals("")) {
            if (count != 0) {
                sql = sql + " and mid in (select mid from directing where did = " +
                        "( select did from director where name = '" + directorName + "'))";
            } else {
                sql = sql + " where mid in (select mid from directing where aid = " +
                        "( select did from director where name = '" + directorName + "'))";
                count += 1;
            }
        }
        if (!start.equals("")) {
            if (count != 0) {
                sql = sql + " and to_char(rdate,'yyyy-mm-dd') > " + "'" + start + "'";
            } else {
                sql = sql + " where to_char(rdate,'yyyy-mm-dd') > " + "'" + start + "'";
                count += 1;
            }
        }
        if (!end.equals("")) {
            if (count != 0) {
                sql = sql + " and to_char(rdate,'yyyy-mm-dd') < " + "'" + end + "'";
            } else {
                sql = sql + " where to_char(rdate,'yyyy-mm-dd') < " + "'" + end + "'";
                count += 1;
            }
        }
        if (!type.equals("")) {
            if (count != 0) {
                sql = sql + "and type = '" + type + "'";
            } else {
                sql = sql + " where type = '" + type + "'";
            }
        }
        return sql;
    }

    /*
    //返回所有表
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
    public Long queryMovieNumByYear(@RequestParam(value = "year") String year) throws Exception {
        Long movieNum = 0L;
        Statement statement = druidDataSource.getConnection().createStatement();
        String sql = "select * from movie";
        ResultSet res = statement.executeQuery(sql);
        while (res.next()) {
            if (res.getString(2) != null && res.getString("RDaTE").length() > 5&&res.getString("RDaTE").substring(0, 4).equals(year)) {
                movieNum++;
            }
        }
        return movieNum;
    }

    //XX年XX月有多少电影
    @RequestMapping("/selectbytime/movienumbymonth")
    public Long queryMovieNumByMonth(@RequestParam(value = "year") String year,@RequestParam(value = "month") String month){
        try {
            Long movieNum = 0L;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from movie";
            String time = year + '-'+ month;
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                if(res.getString(2)!=null&&res.getString(2).length() > 6&&res.getString(2).substring(0,7).equals(time)){
                    movieNum++;
                }
            }
            return movieNum;
        }catch (Exception e){
            return null;
        }
    }

    //XX导演有多少电影
    @RequestMapping("/selectbydirector/movienum")
    public int queryMovieNumByDirector(@RequestParam(value = "name") String name){
        try {
            int movieNum = 0;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select count(MID) from directing join director on (director.did=directing.did) " +
                    "where director.name="+'\"' +name+'\"';
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                movieNum = res.getInt(1);
            }
            return movieNum;
        }catch (Exception e){
            return -1;
        }
    }

    //XX演员的全部信息
    @RequestMapping("/queryactor")
    public List queryActorAllInfo(@RequestParam(value = "name") String name){
        try {
            List resultList = new ArrayList();
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select name, rdate, type, mid from " +
                    "(movie join acting on (m.mid=a.aid)) as a join actor on a.aid=actor.aid" +
                    "where actor.name="+'\"'+name+'\"';
            ResultSet res = statement.executeQuery(sql);
            List list1 = new ArrayList();
            while(res.next()){
                Movie movie = new Movie();
                movie.setName(res.getString("name"));
                movie.setType(res.getString("type"));
                movie.setmId(res.getString("mid"));
                movie.setrDate(res.getString("rdate"));
                list1.add(movie);
            }
            resultList.add(list1);
            return list1;
        }catch (Exception e){
            return null;
        }
    }

    //XX演员演了多少电影
    @RequestMapping("/selectbyactor/movienum")
    public int queryMovieNumByActor(@RequestParam(value = "name") String name){
        try {
            int movieNum = 0;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select count(MID) from acting join actor " +
                    "on (acting.aid=actor.aid" +
                    "where actor.name="+'\"'+name+'\"';
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                movieNum = res.getInt(1);
            }
            return movieNum;
        }catch (Exception e){
            return -1;
        }
    }

    //某种类型电影每天的上映数
    @RequestMapping("/selectbytype/movienum")
    public List queryMovieNumByType(@RequestParam(value = "type") String type){
        List resultList = new ArrayList();
        try {
            List tempList = new ArrayList();
            long movieNum = 0L;
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select m.mymonth, m.myday, count(MID) from " +
                    "(select mid , month(rdate) as mymonth, day(rdate) as myday from movie where type="+type+") as m " +
                    "group by m.mymonth,m.myday";
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                tempList.add(res.getLong(1));
                tempList.add(res.getLong(2));
                tempList.add(res.getLong(3));
                resultList.add(tempList);
            }
            return resultList;
        }catch (Exception e){
            return null;
        }
    }

    //查找电影
    @RequestMapping("/select/movie")
    public List queryMovie(@RequestParam(value = "name") String name, @RequestParam(value = "actorname") String actorName,
                           @RequestParam(value = "start") String start, @RequestParam(value = "directorname") String directorName,
                           @RequestParam(value = "end") String end, @RequestParam(value = "catalog") String type){
        try {
            List<Movie> resultList = new ArrayList<Movie>();
            Statement statement = druidDataSource.getConnection().createStatement();
            String sql = "select * from movie where ";
            boolean empty = true;
            if(!name.equals("")){
                    sql = sql + "name=" + name;
                    empty = false;
            }
            if (!actorName.equals("")){
                sql = sql + " and mid in (select mid from acting where aid = " +
                        "( select aid from actor where name = " + actorName + "))";
                empty = false;
            }
            if(!start.equals("")){
                sql = sql + " and rdate > " + start;
                empty = false;
            }
            if(!end.equals("")){
                sql = sql + " and rdate < " + end;
                empty = false;
            }
            if(!type.equals("")){
                sql = sql + "and type =" + type;
                empty = false;
            }
            if(empty){
                sql = "select * from movie";
            }
            ResultSet res = statement.executeQuery(sql);
            while(res.next()){
                Movie movie = new Movie();
                movie.setmId(res.getString("mid"));
                movie.setName(res.getString("name"));
                movie.setrDate(res.getString("rdate"));
                movie.setType(res.getString("type"));
                resultList.add(movie);
            }
            return resultList;
        }catch (Exception e){
            return null;
        }
    }

*/
}
