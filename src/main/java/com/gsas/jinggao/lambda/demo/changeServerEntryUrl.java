package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.services.lambda.runtime.events.SNSEvent;

public class changeServerEntryUrl implements RequestHandler<SNSEvent, String> {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gameSvrInfo";
	static final String USER = "root";
	static final String PASS = "abcd1234";
	

    @Override
    public String handleRequest(SNSEvent event, Context context) {
    	context.getLogger().log("Received event: " + event);
        String message = event.getRecords().get(0).getSNS().getMessage();
        context.getLogger().log("From SNS: " + message);
        
        JSONObject object = JSONObject.parseObject(message);
        String eventType = object.getString("Event");
        if(eventType.contains("EC2_INSTANCE_TERMINATE")){
        	//服务器关闭成功
        	String instanceId = object.getString("EC2InstanceId");
        	context.getLogger().log("as eventType="+eventType+" ec2InstanceId="+instanceId);
        	Connection conn = initDbConnection(context);
        	
        	//获取合服信息
        	MergeInfo info = getServerInfo(context,conn,instanceId);
        	if(info.sourceServerId!=0) {
        		//更新服务器列表数据
        		updateServerConfig(context,conn,info);
        		//完成合服
        		completeMergeServer(context,conn,info.sourceServerId);
        	}
        	closeDbConnection(context, conn);
        }
        
        
        return message;
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
    
    private class MergeInfo{
		public int sourceServerId;
		public int targetServerId;
		public String newUrl;
		public String newInstanceId;
	}
    
    private MergeInfo getServerInfo(Context context, Connection conn, String instanceId) {
    	PreparedStatement stmt = null;
		MergeInfo minfo = new MergeInfo();
		try {
			stmt = conn.prepareStatement("select * from server_merge_info where isDone=0 and srcInstanceId=?");
			stmt.setString(1, instanceId);
			ResultSet rs = stmt.executeQuery();
			// 展开结果集数据库
			if (rs.next()) {
				// 通过字段检索
				minfo.sourceServerId = rs.getInt("id");
				minfo.targetServerId = rs.getInt("targetServerId");
				minfo.newUrl = rs.getString("newurl");
				minfo.newInstanceId = rs.getString("newInstanceId");
			}
			// 完成后关闭
			rs.close();
			stmt.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
		return minfo;
	}
    
    private void updateServerConfig(Context context, Connection conn, MergeInfo mInfo) {
    	PreparedStatement stmt = null;
    	try {
			stmt = conn.prepareStatement("update svrInfo_config set url=?,instanceId=?,isOpen=1 where id=?");
			stmt.setString(1, mInfo.newUrl);
			stmt.setString(2, mInfo.newInstanceId);
			stmt.setInt(3, mInfo.sourceServerId);
			stmt.executeUpdate();
			stmt.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
    }
    
    private void completeMergeServer(Context context, Connection conn, int serverId) {
    	PreparedStatement stmt = null;
    	try {
			stmt = conn.prepareStatement("update server_merge_info set isDone=1 where id=?");
			stmt.setInt(1, serverId);
			stmt.executeUpdate();
			stmt.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
    }
}
