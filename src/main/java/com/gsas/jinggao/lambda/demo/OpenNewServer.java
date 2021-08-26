package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import java.sql.*;

public class OpenNewServer implements RequestHandler<SNSEvent, String> {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gameSvrInfo";
	static final String USER = "root";
	static final String PASS = "abcd1234";
	

    @Override
    public String handleRequest(SNSEvent event, Context context) {
        context.getLogger().log("Received event: " + event);
        String message = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("openNewServer function v1.0.2");
        context.getLogger().log("From SNS: " + message);
        
        JSONObject object = JSONObject.parseObject(message);
        String eventType = object.getString("Event");
        if(eventType.contains("EC2_INSTANCE_LAUNCH")){
        	//服务器启动成功
        	String instanceId = object.getString("EC2InstanceId");
        	context.getLogger().log("as eventType="+eventType+" ec2InstanceId="+instanceId);
        	
        	String publicDns = GetDNS(instanceId);
        	if(!publicDns.isEmpty()) {
        		Connection con = initDbConnection(context);
        		if(con!=null) {
        			int curSvrId = queryCurSvrIdFromRDS(context,con);
        			//老服不推荐
        			if(curSvrId!=0){
        				updateRecommendCurSvr(context,con,curSvrId);
        			}
        			
        			//生成新服列表
        			int newSvrId = curSvrId+1;
        			int newIndex = curSvrId+1;
        			String newSvrName = "GameArea "+newSvrId+" server";
        			createServer(context,con,newSvrId,newIndex,newSvrName,publicDns,instanceId);
        			
        			closeDbConnection(context,con);
        		}
        	}
        }
        
        
        return message;
    }
    
    private String GetDNS(String aInstanceId)
    {
      String publicDns = "";
      DescribeInstancesRequest request = new DescribeInstancesRequest();
      request.withInstanceIds(aInstanceId);
      AmazonEC2 ec2 = AmazonEC2ClientBuilder.defaultClient();
      DescribeInstancesResult result = ec2.describeInstances(request);

      for (Reservation reservations : result.getReservations())
      {
        for (Instance instance : reservations.getInstances())
        {
          if (instance.getInstanceId().equals(aInstanceId))
          {
        	  publicDns = instance.getPublicDnsName();
        	  break;
          }
        }
      }

      return publicDns;
    }
    
    private Connection initDbConnection(Context context) {
    	Connection conn = null;
    	 try {
	    	 Class.forName(JDBC_DRIVER);
	    	 context.getLogger().log("Starting to connect db!");
	    	 conn = DriverManager.getConnection(DB_URL,USER,PASS);
	    	 context.getLogger().log("connect db succdess!");
	     }catch(SQLException e) {
	    	 context.getLogger().log(e.toString());
	     }catch(Exception e) {
	    	 context.getLogger().log(e.toString());
	     }
    	 return conn; 	
    }
    
    private void closeDbConnection(Context context,Connection con) {
    	if(con!=null) {
    		try {
				con.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				context.getLogger().log(e.toString());
			}
    	}
    }
    
	public boolean createServer(Context context, Connection conn, int svrId,int index,String name,String url,String instanceId) {
		PreparedStatement stmt = null;
		boolean ret = true;
		try {
			//INSERT INTO `svrInfo_config` VALUES (1, 1, 0, 1,'北京1区','');
			stmt = conn.prepareStatement("insert into svrInfo_config values(?,?,?,?,?,?,?)");
			stmt.setInt(1, svrId);
			stmt.setInt(2, index);
			stmt.setInt(3, 1);
			stmt.setInt(4, 1);
			stmt.setString(5, name);
			stmt.setString(6, url);
			stmt.setString(7, instanceId);
			stmt.executeUpdate();
			
			stmt.close();
		} catch (Exception e) {
			context.getLogger().log(e.toString());
			ret = false;
		} 
		return ret;
	}
	
	public void updateRecommendCurSvr(Context context, Connection conn, int svrId){
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("update svrInfo_config set recommend=0 where id=?");
			stmt.setInt(1, svrId);
			stmt.executeUpdate();
			
			stmt.close();
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		} 
	}
	
    
	private int queryCurSvrIdFromRDS(Context context, Connection conn) {
		Statement stmt = null;
		int curSvrId = 0; // 当前推荐服务器
		try {
			stmt = conn.createStatement();
			String sql = "SELECT * FROM svrInfo_config where recommend=1 limit 1"; // 确定当前服务器id
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
		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
		return curSvrId;
	}
}
