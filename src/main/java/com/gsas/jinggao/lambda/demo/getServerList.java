package com.gsas.jinggao.lambda.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class getServerList implements RequestHandler<Object, String> {
	
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://gamesvrinfodb.cluster-cm8uyqw2k3bt.rds.cn-northwest-1.amazonaws.com.cn:3306/gameSvrInfo";
	static final String USER = "root";
	static final String PASS = "abcd1234";
	

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Input: " + input);
        
        ArrayList<ServerInfo> sInfoList = getServerInfoList(context);
        
        //服务器列表 JSON
        JSONObject data = new JSONObject();
		try {
			ArrayList<HashMap<String, Object>> serverList = new ArrayList<HashMap<String, Object>>();
			for (ServerInfo sInfo : sInfoList) {
			HashMap<String, Object> serverData = new HashMap<String, Object>();
				serverData.put("serverId", sInfo.id);
				serverData.put("index", sInfo.index);
				serverData.put("recommend", sInfo.recommend);
				serverData.put("isOpen", sInfo.isOpen);
				serverData.put("name", sInfo.name);
				serverData.put("url", sInfo.url);
				serverList.add(serverData);
			}
			data.put("serverList", serverList);
		} catch (JSONException e) {
			context.getLogger().log(e.toString());
		}
		context.getLogger().log(data.toJSONString());
		return data.toJSONString();
    }
    
    //获取服务器列表
    private ArrayList<ServerInfo> getServerInfoList(Context context){
    	
    	Connection conn = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		ArrayList<ServerInfo> serverInfoList = new ArrayList<ServerInfo>();
		try {
			Class.forName(JDBC_DRIVER);
			context.getLogger().log("Starting to connect db!");
			conn = DriverManager.getConnection(DB_URL, USER, PASS);
			context.getLogger().log("connect db succdess!");

			// 确定当前推荐服有多少注册用户
			stmt = conn.prepareStatement("SELECT * FROM svrInfo_config");
			rs = stmt.executeQuery();

			// 展开结果集数据库
			while (rs.next()) {
				// 通过字段检索
				ServerInfo sInfo = new ServerInfo();
				sInfo.id = rs.getInt("id");
				sInfo.index = rs.getInt("index");
				sInfo.recommend = rs.getInt("recommend");
				sInfo.isOpen = rs.getInt("isOpen");
				sInfo.name = rs.getString("name");
				sInfo.url = rs.getString("url");
				
				serverInfoList.add(sInfo);
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
		return serverInfoList;
    }
    
    private class ServerInfo{
    	public int id;
    	public int index;
    	public int recommend;
    	public int isOpen;
    	public String name;
    	public String url;
    }

}
