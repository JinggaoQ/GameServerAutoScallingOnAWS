package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClientBuilder;
import com.amazonaws.services.autoscaling.model.TerminateInstanceInAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.TerminateInstanceInAutoScalingGroupResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class mergeServer implements RequestHandler<Object, String> {

	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gameSvrInfo";
	static final String USER = "root";
	static final String PASS = "abcd1234";
	

	static final String LOG_DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gamelog";
	static final String LOG_USER = "root";
	static final String LOG_PASS = "abcd1234";
	
    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        String response = "mergeServer Success!";
        Connection conn = initDbConnection(context);
        if(conn == null) {
        	response = "DB connection failed!";
        	return response;
        }
        ArrayList<Integer> tobeMergedServerList = getTobeMergedServerList(context, conn);
        for(int serverId : tobeMergedServerList) {
        	//关闭入口
        	closeServerEntryPoint(context, conn, serverId);
        	//停服
        	terminateServer(context, conn, serverId);
        }
        closeDbConnection(context, conn);
        return response;
    }
    
    private void terminateServer(Context context, Connection conn, int serverId) {
    	
    	String instanceId="";
		//获取server的ec2 instanceId
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("select instanceId from svrInfo_config where id=?");
			stmt.setInt(1, serverId);
			ResultSet rs = stmt.executeQuery();
			// 展开结果集数据库
			if (rs.next()) {
				// 通过字段检索
				instanceId = rs.getString("instanceId");
			}
			// 完成后关闭
			rs.close();
			stmt.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
		
		//关闭ec2
		if(!instanceId.isEmpty()) {
			AmazonAutoScaling client = AmazonAutoScalingClientBuilder.standard().build();
			TerminateInstanceInAutoScalingGroupRequest request = new TerminateInstanceInAutoScalingGroupRequest().withInstanceId(instanceId)
			        .withShouldDecrementDesiredCapacity(true);
			TerminateInstanceInAutoScalingGroupResult response = client.terminateInstanceInAutoScalingGroup(request);
			context.getLogger().log("terminating instance response:"+response.toString());
		}
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
    
    /**
     * 关闭大区入口
     * @param serverId
     */
    private void closeServerEntryPoint(Context context,Connection conn, int serverId) {
    	PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("update svrInfo_config set isOpen=0 where id=?");
			stmt.setInt(1, serverId);
			stmt.executeUpdate();
			
			stmt.close();
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		} 
    }
    
    
    /**
     * 获取等待合服的列表
     * @param context
     * @return
     */
	private ArrayList<Integer> getTobeMergedServerList(Context context, Connection conn) {
		Statement stmt = null;
		ArrayList<Integer> tobeMergedServerId = new ArrayList<Integer>();
		try {
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT id FROM server_merge_info where isDone=0"; // 还未完成合服操作的大区
			ResultSet rs = stmt.executeQuery(sql);
			// 展开结果集数据库
			while (rs.next()) {
				// 通过字段检索
				int id = rs.getInt("id");
				tobeMergedServerId.add(id);
			}
			// 完成后关闭
			rs.close();
			stmt.close();

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
		return tobeMergedServerId;
	}

}
