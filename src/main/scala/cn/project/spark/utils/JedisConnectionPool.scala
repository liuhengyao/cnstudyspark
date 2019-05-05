package cn.project.spark.utils

import java.util
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  private val config: JedisPoolConfig = new JedisPoolConfig()

  //最大连接数
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //获取连接是检查连接的有效性
  config.setTestOnBorrow(true)

  val pool = new JedisPool(config, "node1", 6379)

  def getConnetion(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val conn = JedisConnectionPool.getConnetion()
    val res: util.Set[String] = conn.keys("*")
  }
}
