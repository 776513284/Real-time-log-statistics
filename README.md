# Real-time-log-statistics
软件架构：Sparkstreaming，Zookeeper、Flume、Kafka、Hbase、SSM
项目描述：本项目是一个仿爱奇艺视频网站实时日志信息统计的项目，通过实时产生的后台日志，统计出今天到现在为止的不同视频类别的访问量以及从搜索引擎引流过来的类别的访问量。
实现技术：编写python脚本产生模拟日志信息存放于linux目录下，通过cron调度每隔一分钟产生后台日志至linux目录下，通过flume收集日志到kafka集群主题上，通过sparkstreaming收集kafka主题信息到hbase,通过基于springboot框架和echart页面进行展示。

架构图
![image](https://github.com/776513284/Real-time-log-statistics/blob/master/%E5%9B%BE%E7%89%87/%E6%9E%B6%E6%9E%84%E5%9B%BE.png)
