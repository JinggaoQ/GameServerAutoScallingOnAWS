package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class identifyToBeMergedServer implements RequestHandler<Object, String> {
	
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

        ArrayList<Integer> tobeMergedList = getTobeMergedServerList(context);
        generateMergeInfos(context, tobeMergedList);
        return "Hello from Lambda!";
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
    
    private void generateMergeInfos(Context context, ArrayList<Integer> tobeMergedServerList) {
    	
    	//生成 原服务器合目标服务器的关系
    	HashMap<Integer,Integer> sourceTargetMap = new HashMap<Integer,Integer>();
    	int i=0;
    	int size = tobeMergedServerList.size();
    	while(i<size) {
    		int target = tobeMergedServerList.get(i);
    		++i;
    		if(i<size) {
    			int source = tobeMergedServerList.get(i);
    			sourceTargetMap.put(source, target);
    			++i;
    		}
    	}
    	
    	Connection conn = initDbConnection(context);
    	for(int source : sourceTargetMap.keySet()) {
    		int target = sourceTargetMap.get(source);
    		addMergeInfo(context,conn,source,target);
    	}
    	closeDbConnection(context,conn);
    }
    
    private void addMergeInfo(Context context, Connection conn, int sourceId, int targetId) {
    	PreparedStatement stmt = null;
    	ResultSet rs = null;
    	
		String srcServerName ="";
		String srcInstanceId ="";
		String newUrl = "";
		String newInstanceId = "";
		try {
			
			// 确定原服的名称
			stmt = conn.prepareStatement("SELECT name,instanceId FROM svrInfo_config WHERE id = ?");
			stmt.setInt(1, sourceId);
			rs = stmt.executeQuery();

			// 展开结果集数据库
			if (rs.next()) {
				// 通过字段检索
				srcServerName = rs.getString("name");
				srcInstanceId = rs.getString("instanceId");
			}
			// 完成后关闭
			rs.close();
			stmt.close();
			
			if(!srcServerName.isEmpty()) {
				//确定目标服的serverId, url, instanceId
				PreparedStatement stmt2 = null;
				ResultSet rs2 = null;
				stmt2 = conn.prepareStatement("select url,instanceId from svrInfo_config where id=?");
				stmt2.setInt(1, targetId);
				rs2 = stmt2.executeQuery();
				if(rs2.next()) {
					newUrl = rs2.getString("url");
					newInstanceId = rs2.getString("instanceId");
				}
				
				rs2.close();
				stmt2.close();
				
				if(newUrl!=null) {
					//插入mergeInfo信息
					//INSERT INTO `server_merge_info` VALUES (2,'北京2区',1,'url','instanceId',0);
					PreparedStatement stmt3 = null;
					stmt3 = conn.prepareStatement("insert into server_merge_info values(?,?,?,?,?,?,0)");
					stmt3.setInt(1, sourceId);
					stmt3.setString(2, srcServerName);
					stmt3.setString(3, srcInstanceId);
					stmt3.setInt(4, targetId);
					stmt3.setString(5, newUrl);
					stmt3.setString(6, newInstanceId);
					stmt3.executeUpdate();
					
					stmt3.close();
				}
			}

		} catch (SQLException e) {
			context.getLogger().log(e.toString());
		} catch (Exception e) {
			context.getLogger().log(e.toString());
		}
    }
    
    private ArrayList<Integer> getTobeMergedServerList(Context context) {	
		Connection conn = null;
		Statement stmt = null;
		ArrayList<Integer> tobeMergedServerId = new ArrayList<Integer>();
		try {
			Class.forName(JDBC_DRIVER);
			context.getLogger().log("Starting to connect db!");
			conn = DriverManager.getConnection(LOG_DB_URL, LOG_USER, LOG_PASS);
			context.getLogger().log("connect logdb succdess!");
			stmt = conn.createStatement();
			String sql;
			sql = "SELECT iServerId FROM DailyActiveUser where iDAU<1000"; // 活跃少于1000的服
			// 
			ResultSet rs = stmt.executeQuery(sql);
			// 展开结果集数据库
			while (rs.next()) {
				// 通过字段检索
				int id = rs.getInt("iServerId");
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
