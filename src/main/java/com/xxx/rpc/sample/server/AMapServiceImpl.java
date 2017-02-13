package com.xxx.rpc.sample.server;

import com.xxx.rpc.sample.api.AMapService;
import com.xxx.rpc.server.RpcService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by MK33 on 2016/11/23.
 */
@RpcService(AMapService.class)
public class AMapServiceImpl implements AMapService {
    @Override
    public String request(String url) throws IOException {
        URL u = new URL(url);
        HttpURLConnection httpURLConnection = (HttpURLConnection) u.openConnection();
        InputStream in = httpURLConnection.getInputStream();
        BufferedReader bf = new BufferedReader(new InputStreamReader(in));
        StringBuffer sb = new StringBuffer();
        String tmp = bf.readLine();
        while (tmp != null) {
            sb.append(tmp);
            tmp = bf.readLine();
        }
        return sb.toString();
    }
}
