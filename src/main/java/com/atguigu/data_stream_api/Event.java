package com.atguigu.data_stream_api;

/**
 * ClassName: Event
 * Package: com.atguigu.data_stream_api
 * Description:
 *
 * @Author: fenghao
 * @Create 2023/10/29 11:32
 * @Version 1.0
 */

// 用户的每次点击事件数据源定义
public class Event {
    public String user;
    public String url;
    public Long timestamp;
    public Event(){
    }

    public  Event(String user,String url, Long timestamp) {
    this.user = user;
    this.url = url;
    this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}




