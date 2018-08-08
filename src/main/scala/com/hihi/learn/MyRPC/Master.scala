package com.hihi.learn.MyRPC

import akka.actor.{Actor, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.collection.mutable
import scala.concurrent.duration._
import scala.io.StdIn

/**
  * Created by root on 2016/5/13.
  */
class Master extends Actor {

  // workerId -> WorkerInfo
  val idToWorker = new mutable.HashMap[String, WorkerInfo]()
  // WorkerInfo
  val workers = new mutable.HashSet[WorkerInfo]() //使用set删除快, 也可用linkList
  //超时检查的间隔
  val CHECK_INTERVAL = 17000

  override def preStart(): Unit = {
    println("preStart invoked")
    import context.dispatcher //使用timer太low了, 可以使用akka的, 使用定时器, 要导入这个包
    context.system.scheduler.schedule(0 millis, CHECK_INTERVAL millis, self, CheckTimeOutWorker)
  }

  // 用于接收消息
  override def receive: Receive = {
    case RegisterWorker(id ,memory, cores) => {
      printf("Create a new connection, wokerId is %s, memory is %d, cores is %d", id, memory, cores)
      idToWorker(id) = new WorkerInfo(id, memory, cores)
      workers += idToWorker(id)

      sender ! RegisteredWorker(s"akka.tcp://MasterSystem@${Master.host}:${Master.port}/user/Master")
    }
    case Heartbeat(id) => {
      if (idToWorker.contains(id)) {
        idToWorker(id).lastHeartbeatTime =  System.currentTimeMillis()
      }
    }
    case CheckTimeOutWorker => {
      val toRemove = workers.filter(System.currentTimeMillis() - _.lastHeartbeatTime > CHECK_INTERVAL)
      for(w <- toRemove) {
        workers -= w
        idToWorker -= w.id
      }
      println(workers.size)
    }

  }
}

object Master {
  val host = "127.0.0.1"
  val port = 12345

  def main(args: Array[String]) {

    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("MasterSystem", config)
    //创建Actor, 起个名字
    val master = actorSystem.actorOf(Props[Master], "Master")//Master主构造器会执行
    StdIn.readLine
    actorSystem.terminate()  //让进程等待着, 先别结束
  }
}
