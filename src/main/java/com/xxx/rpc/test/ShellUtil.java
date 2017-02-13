package com.xxx.rpc.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Created by MK33 on 2016/12/7.
 */
public class ShellUtil {

    public static void main(String args[]) throws IOException, InterruptedException {

        String s = "/opt/spark/apache-mahout-distribution-0.12.2/bin/mahout  recommenditembased  --input /tmp/kdj/shop_goods_rating  --output /tmp/kdj/mahout_recommend " +
                "--numRecommendations 2000 " +
                " --maxPrefsPerUser 1000 " +
                " --maxSimilaritiesPerItem 1000 " +
                " --similarityClassname SIMILARITY_LOGLIKELIHOOD " +
                " --tempDir /tmp/kdj/mahout_tmp ";

        Process process = Runtime.getRuntime().exec(s);
        InputStream in = process.getInputStream();
        BufferedReader buffer = new BufferedReader(new InputStreamReader(in));
        String tmp = buffer.readLine();
        while (tmp != null) {
            System.out.println(tmp);
            tmp = buffer.readLine();
        }
        process.waitFor();

    }
}
