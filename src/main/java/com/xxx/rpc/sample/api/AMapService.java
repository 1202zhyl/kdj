package com.xxx.rpc.sample.api;

import java.io.IOException;
import java.net.MalformedURLException;

/**
 * 请求 AMap Request
 * Created by MK33 on 2016/11/23.
 */
public interface AMapService {

    public String request(String url) throws IOException;
}
