package org.pengfei.Lesson6_Spark_Streaming

import java.io.PrintWriter
import java.net.ServerSocket

import scala.io.Source

object SteamingSource {
  def index(n:Int)=scala.util.Random.nextInt(n)

  def main(args:Array[String]): Unit ={
    // This object main takes three arguments, 1st is filePath, 2nd port number, 3rd is timeInterval
    // verify args length
    if(args.length !=3){
      System.err.println("Usage: <fileName> <port> <millisecond>")
      System.exit(1)
    }
    // assign args
    val fileName= args(0)
    val lines= Source.fromFile(fileName).getLines().toList
    val fileRowNum=lines.length
    val port:Int=args(1).toInt
    val timeInterval:Long=args(2).toLong

    // set up server socket with the given port
    val listener = new ServerSocket(port)

    //always running and waiting for new client connection
    while(true){
      // When a new client connected, starts a new thread to treat client request
      val socket=listener.accept()

      new Thread(){
        override def run={
          // get client info
          println("Got client connected from: " + socket.getInetAddress)

          val out = new PrintWriter(socket.getOutputStream(), true)

          while(true){
            Thread.sleep(timeInterval)
            //get a random line of the file and send it to client
            val content=lines(index(fileRowNum))
            println("-------------------------------------------")
            println(s"Time: ${System.currentTimeMillis()}")
            println("-------------------------------------------")
            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
