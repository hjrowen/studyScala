package com.hihi.learn.MyRPC

import java.util.UUID

import akka.actor.{Actor, ActorSelection, ActorSystem, Props}
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import scala.io.StdIn

/**
  * Created by root on 2016/5/13.
  */
class Worker(val masterHost: String, val masterPort: Int) extends Actor{

  var master : ActorSelection = _
  val workerId = UUID.randomUUID().toString
  val HEART_INTERVAL = 10000

  //建立连接
  override def preStart(): Unit = {
    //在master启动时会打印下面的那个协议, 可以先用这个做一个标志, 连接哪个master
    //继承actor后会有一个context, 可以通过它来连接
    master = context.actorSelection(s"akka.tcp://MasterSystem@$masterHost:$masterPort/user/Master") //需要有/user, Master要和master那边创建的名字保持一致
    master ! RegisterWorker(workerId, 4 * 1024 * 1024, 2)
  }

  override def receive: Receive = {
    case RegisteredWorker(masterUrl: String) => {
      printf("connect success masterUrl is %s.", masterUrl)
      //启动定时器发送心跳
      import context.dispatcher
      //多长时间后执行 单位,多长时间执行一次 单位, 消息的接受者(直接给master发不好, 先给自己发送消息, 以后可以做下判断, 什么情况下再发送消息), 信息
      context.system.scheduler.schedule(0 millis, HEART_INTERVAL millis, self, SendHeartbeat)
    }
    case SendHeartbeat => {
      println("send heartbeat to master")
      master ! Heartbeat(workerId)
    }
  }
}

object Worker {
  def main(args: Array[String]) {
    val host = "127.0.0.1"
    val port = 12346
    val masterHost = "127.0.0.1"
    val masterPort = 12345
    // 准备配置
    val configStr =
      s"""
         |akka.actor.provider = "akka.remote.RemoteActorRefProvider"
         |akka.remote.netty.tcp.hostname = "$host"
         |akka.remote.netty.tcp.port = "$port"
       """.stripMargin
    val config = ConfigFactory.parseString(configStr)
    //ActorSystem老大，辅助创建和监控下面的Actor，他是单例的
    val actorSystem = ActorSystem("WorkerSystem", config)
    actorSystem.actorOf(Props(new Worker(masterHost, masterPort)), "Worker")
    StdIn.readLine
    actorSystem.terminate()
  }
}

