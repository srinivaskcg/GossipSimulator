import akka.actor.{ ActorRef, ActorSystem, Props, Actor }
import scala.math._
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import java.io.PrintWriter
import scala.concurrent.duration._
import akka.actor.Scheduler
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import akka.actor.Cancellable
import scala.concurrent.ExecutionContext.Implicits.global

//Messages
sealed trait GossipMessage

case class InitializeGossip(numOfNodes: Int, topology: String, nodeActor: ArrayBuffer[ActorRef], actorSystem: ActorSystem, rumorCount: Int) extends GossipMessage
case class InitializePushSum(numOfNodes: Int, topology: String, nodeActor: ArrayBuffer[ActorRef], actorSystem: ActorSystem, s: Int, w: Int) extends GossipMessage

case class Gossip() extends GossipMessage

case class PushSum(_msgS: Double, _msgW: Double) extends GossipMessage


//project 2
object project2 {
  
  var startTime : Long = System.currentTimeMillis()
  var finishTime : Long = 0
  var nodeCounter : Int = 0
  def main(args: Array[String]) {
    
    if (args.length != 3) {
      println("Enter the number of nodes, topology (full, 3D, line or imp3D) and algorithm (gossip or push-sum).")
      System.exit(1)
    }
    else {
      if((args(1).equalsIgnoreCase("full") || args(1).equalsIgnoreCase("3D") || args(1).equalsIgnoreCase("line") || args(1).equalsIgnoreCase("imp3D"))
         &&
         (args(2).equalsIgnoreCase("gossip") || args(2).equalsIgnoreCase("push-sum")))
      {
          val system = ActorSystem("GossipSimulatorSystem")
          var topologyNodes : Int = 0
          var numOfNodes : Int = args(0).toInt
          var topology : String = args(1).toString()
          var algorithm : String = args(2).toString()
          val writer = new PrintWriter("test.txt")
          
          println("numOfNodes:"+numOfNodes+"--Topology:"+topology+"--algorithm"+algorithm);
          
          if (topology == "3D" || topology == "imp3D"){
              val checkCube = floor(cbrt(numOfNodes)).toInt
              topologyNodes = pow(checkCube, 3).toInt 
          }
          else
              topologyNodes = numOfNodes
            
          var nodeActors = new ArrayBuffer[ActorRef]()
                    
          for (i <- 0 until topologyNodes) {
            nodeActors += system.actorOf(Props(new Node(writer)), name = "nodeActor" + i) 
          }
                    
          if("gossip" == algorithm){
              
            println("Start Gossipping")
            
            for (i <- 0 until topologyNodes) 
              nodeActors(i) ! InitializeGossip(topologyNodes, topology, nodeActors, system, 0)
        
            nodeActors(0) ! Gossip()
            
          }else if("push-sum" == algorithm){
              
            println("Start push sum")
            
            for (i <- 0 until topologyNodes) 
              nodeActors(i) ! InitializePushSum(topologyNodes, topology, nodeActors, system, i, 1)
              
            nodeActors(0) ! PushSum(0,1)
          }else{
              println("Entered Algorithm is invalid. Select gossip or push-sum.")
              System.exit(1)
          }
      }//If
    }//else
  }//main
  
  class Node(var writer: PrintWriter) extends Actor {
  
  var rumorCount : Int = 0
  var rumorLimit : Int = 10
  var currentNode : String = ""
  var numOfNodes : Int = 0
  var nbrNodeList = new ArrayBuffer[Int]
  var actorSystem : ActorSystem = null  
  var nodeActors : ArrayBuffer[ActorRef] = null
  var topology : String = ""
  var actorName : String = ""
  var schedulor : akka.actor.Cancellable = _
  var tickFlag= 0

  var myS : Double = 0
  var myW : Double = 1
  var msgS : Double = 0
  var msgW : Double = 1  
  var mySbymyW : Double = 0
  var myPrevSbymyPrevW : Double = 0
  var pushSumConvergence : Int = 0
  
  def receive = {
    
    case InitializeGossip(_numOfNodes, _topology, _nodeActors, _actorSystem, _rumorCount) => {

      numOfNodes = _numOfNodes
      topology = _topology
      actorName = self.path.name.toString()
      actorSystem = _actorSystem
      nodeActors = _nodeActors
      rumorCount = _rumorCount
    }
    
    case InitializePushSum(_numOfNodes, _topology, _nodeActors, _actorSystem, _s, _w) => {

      numOfNodes = _numOfNodes
      topology = _topology
      actorName = self.path.name.toString()
      actorSystem = _actorSystem
      nodeActors = _nodeActors
      myS = _s
      myW = _w
    }
    
    case Gossip() => {

      var index = nodeActors.indexOf(self)

      if (rumorCount == 0) {
          rumorCount += 1          
          if(tickFlag != 0 ){
            schedulor.cancel()
          }else{
            tickFlag = 1
          }
          
          println("--------------------------------------------------")
          println( self.path.name + "-------rumor count: "+ rumorCount)
          nbrNodeList = getNeighbours(topology, numOfNodes, nodeActors.indexOf(self), nodeActors)
          var randNeighbor = nbrNodeList(Random.nextInt(nbrNodeList.size))
          
          schedulor = context.system.scheduler.schedule( 0 millis, 1000 millis, self, "tickGossip")
          
          nodeActors(randNeighbor) ! Gossip()
       } 
       else if (rumorCount < rumorLimit){
          rumorCount += 1         
          println("--------------------------------------------------")
          println( self.path.name + "-------rumor count: "+ rumorCount)
          nbrNodeList = getNeighbours(topology, numOfNodes, nodeActors.indexOf(self), nodeActors)
          var randNeighbor = nbrNodeList(Random.nextInt(nbrNodeList.size))   
          nodeActors(randNeighbor) ! Gossip()
       }
       else
       {
         nbrNodeList = getNeighbours(topology, numOfNodes, nodeActors.indexOf(self), nodeActors)
         var randNeighbor = nbrNodeList(Random.nextInt(nbrNodeList.size))   
         nodeActors(randNeighbor) ! Gossip()
         
         println(self.path.name+" Terminated!")
         nodeCounter += 1
         val finishTime = System.currentTimeMillis() - startTime
         if(nodeCounter == numOfNodes)
         {
           println("\nConvergence Time::"+finishTime)
         }
         
         println("Number of Nodes Covered :"+ nodeCounter)
         
         context.stop(self)
       }
    }
    
    case "tickGossip" => {
          self ! Gossip()
    }
    
    case PushSum(msgS, msgW) => {
      myS = myS + msgS
      myW = myW + msgW
      
      mySbymyW = myS/myW
      
      myS = myS/2
      myW = myW/2
      
      println("----------------------------------")
      println("Current Node : "+self.path.name)
      println("Current S by W::"+mySbymyW)
      println("Previous S by W::"+myPrevSbymyPrevW)
      println("S/W ratio :"+myS/myW)
      if(Math.abs(mySbymyW-myPrevSbymyPrevW) < Math.pow(10, -10))
        pushSumConvergence += 1
      else
        pushSumConvergence = 0
      
      if(pushSumConvergence == 3)
      {
        var finishTime = System.currentTimeMillis()-startTime
        println("\nCompletion Time :"+finishTime+" milliseconds")
        println("Terminated")
          context.stop(self) 
      }
      else{
          nbrNodeList = getNeighbours(topology, numOfNodes, nodeActors.indexOf(self), nodeActors)
          var randNeighbor = nbrNodeList(Random.nextInt(nbrNodeList.size))
          myPrevSbymyPrevW = myS/myW
      
          nodeActors(randNeighbor) ! PushSum(myS, myW)
      } 
    }
  }
    
    def getNeighbours(topology : String, numOfNodes : Int, actorNode : Int, nodeActors: ArrayBuffer[ActorRef]) : ArrayBuffer[Int] = {
          
          var nbrList = new ArrayBuffer[Int]
          topology match {
              case "full" => nbrList = getFullNeighbours(numOfNodes, actorNode, nodeActors)
              case "line" => nbrList = getLineNeighbours(numOfNodes, actorNode, nodeActors)
              case "3D" => nbrList = get3DNeighbours(numOfNodes, actorNode, nodeActors)
              case "imp3D" => nbrList = getImp3DNeighbours(numOfNodes, actorNode, nodeActors)
              case _ => println("Given topology can't be created")
          }              
          return nbrList
      }
           
      def getFullNeighbours(numOfNodes : Int, selectedNode : Int, nodeActors: ArrayBuffer[ActorRef]) : ArrayBuffer[Int] = {
        
        var nbrList = new ArrayBuffer[Int]
        for (i <- 0 to numOfNodes-1) {
            nbrList += i
        }            
        nbrList -= selectedNode
        return nbrList
      }
      
      def getLineNeighbours(numOfNodes : Int, selectedNode : Int, nodeActors: ArrayBuffer[ActorRef]) : ArrayBuffer[Int] = {
        
        var nbrList = new ArrayBuffer[Int]
             
          if(selectedNode == 0)
            nbrList += (selectedNode+1)
          else if (selectedNode == numOfNodes-1)
            nbrList += (selectedNode-1)
          else 
            nbrList += ((selectedNode+1), (selectedNode-1))
        return nbrList
      }
      
      def get3DNeighbours(numOfNodes : Int, selectedNode : Int, nodeActors: ArrayBuffer[ActorRef]) : ArrayBuffer[Int] = {
        
          var nbrList = new ArrayBuffer[Int]
          var tempNbrList = new ArrayBuffer[Int]
          var gridOrder : Int = cbrt(numOfNodes).toInt
          
          
          tempNbrList += ((selectedNode-1), (selectedNode+1), (selectedNode-gridOrder), (selectedNode+gridOrder),
                                              (selectedNode-(gridOrder*gridOrder)), (selectedNode+(gridOrder*gridOrder)))

          for(i <- 0 to (tempNbrList.size)-1)
          {
            if(nodeActors.isDefinedAt(tempNbrList(i)))
              nbrList += (tempNbrList(i))
          }                
        return nbrList
      }
      
      def getImp3DNeighbours(numOfNodes : Int, selectedNode : Int, nodeActors: ArrayBuffer[ActorRef]) : ArrayBuffer[Int] = {
        
          var nbrList = new ArrayBuffer[Int]
          var tempNbrList = new ArrayBuffer[Int]
          var gridOrder : Int = cbrt(numOfNodes).toInt
          
          tempNbrList += ((selectedNode-1), (selectedNode+1), (selectedNode-gridOrder), (selectedNode+gridOrder),
                                              (selectedNode-(gridOrder*gridOrder)), (selectedNode+(gridOrder*gridOrder)))
                                              
          var randomNode : Int = (selectedNode-1)
          while(tempNbrList.contains(randomNode))
            randomNode = Random.nextInt(numOfNodes)
            
          nbrList += (randomNode)
            
           for(i <- 0 to (tempNbrList.size)-1)
          {
            if(nodeActors.isDefinedAt(tempNbrList(i)))
              nbrList += (tempNbrList(i))
          }      
        return nbrList
      }
  }
}//project 2

