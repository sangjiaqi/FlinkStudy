package Flink1.Untils

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

class Until {

  /*
  *本地调试模式
   */

//  有并行度参数设置的Local运行时创建方法
  def createLocalEnvironment(parallelism: Int = JavaEnv.getDefaultLocalParallelism): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism))
  }

//  除了并行度，还有上下文配置的Local运行时创建方法
  def createLocalEnvironment(parallelism: Int, configuration: Configuration): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironment(parallelism, configuration))
  }

//  创建Local运行时，开启任务监控Web
  def createLocalEnvironmentWithWebUI(config: Configuration = null): StreamExecutionEnvironment = {
    val conf: Configuration = if (config == null) new Configuration() else config
    new StreamExecutionEnvironment(JavaEnv.createLocalEnvironmentWithWebUI(conf))
  }

  /*
  *集群模式
   */

//  有目标集群ip、端口和应用程序包的Remote运行时创建方法
  def createRemoteEnvironment(host: String, port: Int, jarFiles: String*): StreamExecutionEnvironment = {
    new StreamExecutionEnvironment(JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*))
  }

//  除了目标集群ip、端口和程序应用包，还有并行度参数Remote运行时创建方法
  def createRemoteEnvironment(host: String, port: Int, parallelism: Int, jarFiles: String*): StreamExecutionEnvironment = {
    val javaEnv = JavaEnv.createRemoteEnvironment(host, port, jarFiles: _*)
    javaEnv.setParallelism(parallelism)
    new StreamExecutionEnvironment(javaEnv)

  }

}