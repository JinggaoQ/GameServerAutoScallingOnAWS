package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricDataResult;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class PlayerNumMonitor implements RequestHandler<Object, String> {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gameSvrInfo";
	static final String USER = "root";
	static final String PASS = "abcd1234";
	

	static final String LOG_DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gamelog";
	static final String LOG_USER = "root";
	static final String LOG_PASS = "abcd1234";
	

    @Override
    public String handleRequest(Object input, Context context) {
	     int curSvrId = queryCurSvrIdFromRDS(context);
	     if(curSvrId==0) {
	    	 curSvrId=1;  //还未创建过服务器从1服查注册用户量
	     }
	     context.getLogger().log("current server id="+curSvrId);
	     
	     
	     int registerdPlayer = queryRegisterdPlayerNumFromRDS(context,curSvrId);
	     context.getLogger().log("current registerd player number is"+registerdPlayer);
	     
	     putMetric(context, registerdPlayer);
	    
	     
        context.getLogger().log("Input: " + input);
        String output = "Hello, " + input + "!";
        // TODO: implement your handler
        return output;
    }
    
    private void putMetric(Context context, int registerdPlayerNum) {
    	final AmazonCloudWatch cw =
    		    AmazonCloudWatchClientBuilder.defaultClient();

    		/*Dimension dimension = new Dimension()
    		    .withName("UNIQUE_PAGES")
    		    .withValue("URLS");*/

    		MetricDatum datum = new MetricDatum()
    		    .withMetricName("NEW_SERVER_REGISTERD_PLAYER_NUM")
    		    .withUnit(StandardUnit.Count)
    		    .withValue(Double.valueOf(registerdPlayerNum));

    		PutMetricDataRequest request = new PutMetricDataRequest()
    		    .withNamespace("GAME")
    		    .withMetricData(datum);

    		PutMetricDataResult response = cw.putMetricData(request);
    		context.getLogger().log("put metric response:"+response.toString());


    }
    
    private int queryCurSvrIdFromRDS(Context context) {
    	 Connection conn = null;
	     Statement stmt = null;
	     String  output = "";
	     int curSvrId = 0; //当前推荐服务器
    	 try {
	    	 Class.forName(JDBC_DRIVER);
	    	 context.getLogger().log("Starting to connect db!");
	    	 conn = DriverManager.getConnection(DB_URL,USER,PASS);
	    	 context.getLogger().log("connect db succdess!");
	    	 stmt = conn.createStatement();
	         String sql;
	         sql = "SELECT id FROM svrInfo_config where recommend=1 limit 1"; //确定当前服务器id
			ResultSet rs = stmt.executeQuery(sql);
			// 展开结果集数据库
			while (rs.next()) {
				// 通过字段检索
				int id = rs.getInt("id");
				curSvrId = id;
			}
			// 完成后关闭
			rs.close();
			stmt.close();
			conn.close();

	     }catch(SQLException e) {
	    	 context.getLogger().log(e.toString());
	     }catch(Exception e) {
	    	 context.getLogger().log(e.toString());
	     }
    	 return curSvrId;
    }
    
	private int queryRegisterdPlayerNumFromRDS(Context context, int serverId) {
		Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int registerdPlayer = 0;
		try {
			Class.forName(JDBC_DRIVER);
			context.getLogger().log("Starting to connect db!");
			conn = DriverManager.getConnection(LOG_DB_URL, LOG_USER, LOG_PASS);
			context.getLogger().log("connect logdb succdess!");

			// 确定当前推荐服有多少注册用户
			stmt = conn.prepareStatement("SELECT count(0) FROM CreatePlayer WHERE iServerId = ?");
			stmt.setInt(1, serverId);
			rs = stmt.executeQuery();

			// 展开结果集数据库
			if (rs.next()) {
				// 通过字段检索
				int count = rs.getInt(1);
				registerdPlayer = count;
			}
			// 完成后关闭
			rs.close();
			stmt.close();
			conn.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
		return registerdPlayer;
	}

}
