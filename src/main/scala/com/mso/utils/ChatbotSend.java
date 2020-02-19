package com.mso.utils;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class ChatbotSend {
    // 成本数据入库通知
    private static String SUCCESS_WEBHOOK_TOKEN_PAY_DATA = "https://oapi.dingtalk.com/robot/send?access_token=f107229e39254f908172fad324b7b6a3b48449e1747f971a121e62b475f9560f";

    // 【数据分配】/ 【报表异常信息】
    private static String FAIL_WEBHOOK_TOKEN_REPORT = "https://oapi.dingtalk.com/robot/send?access_token=e598948391d45b7bb9540d031eae713ae7baf7bde02de3b18175f960f5b1a158";

    public static void sendPayDataSuccessMessage(String message) {
        sendMessage(message, SUCCESS_WEBHOOK_TOKEN_PAY_DATA,"13564188705");
    }

    public static void sendFailnMessage(String message) {
        sendMessage(message, FAIL_WEBHOOK_TOKEN_REPORT, "13419574030");
    }

    private static void sendMessage(String message, String type, String mobile) {
        try {
            HttpClient httpclient = HttpClients.createDefault();

            HttpPost httppost = new HttpPost(type);
            httppost.addHeader("Content-Type", "application/json; charset=utf-8");

            String textMsg = "{ \"msgtype\": \"text\", \"text\": {\"content\": \"" + message + "\"},\"at\": {\"atMobiles\": [\"" + mobile + "\"],  \"isAtAll\": false}}";

            StringEntity se = new StringEntity(textMsg, "utf-8");
            httppost.setEntity(se);

            HttpResponse response = httpclient.execute(httppost);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String result = EntityUtils.toString(response.getEntity(), "utf-8");
                System.out.println(result);
            }
        } catch (Exception e) {
            System.out.println("数据同步失败，订单id:$project_id");
        }
    }

}
