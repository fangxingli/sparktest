package com.haodou.pig.test
/**
 * watcher.scala
 *
 * Uses the Java 7 WatchEvent filesystem API from within Scala.
 * Based on http://markusjais.com/file-system-events-with-java-7/
 *
 * @author Chris Eberle <eberle1080@gmail.com>
 * @version 0.1 
 */

import scala.collection.JavaConverters._
import util.control.Breaks._
import java.io.{File, IOException}
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.nio.file.FileSystems
import scala.collection.mutable.Map

object directoryWatcher{
    val keysMap : Map[String,scala.collection.immutable.Map[String, String]] = Map()

    def printEvent(rootPath :Path, event:WatchEvent[_]) : Unit = {
        val kind = event.kind
        val event_path = event.context().asInstanceOf[Path]

        kind match {
            case StandardWatchEventKinds.ENTRY_CREATE => println("Entry created: " + rootPath.resolve(event_path))
            case StandardWatchEventKinds.ENTRY_DELETE => println("Entry deleted: " + rootPath.resolve(event_path))
            case StandardWatchEventKinds.ENTRY_MODIFY => {
                if( event_path.getFileName.toString endsWith ".ini" )
                    keysMap.update(event_path.getFileName.toString, parse(new File(rootPath.resolve(event_path).toString)))
                println("Entry modified: " + rootPath.resolve(event_path))
            }
        }
    }

    def start(realpath :String) : Unit = {
        println("Starting watch service...")
        // 开始后先扫描一次检测目录下的所有配置文件
        new File(realpath).listFiles.map(x => (x.getName, parse(x))) foreach { x =>
            keysMap.put(x._1, x._2)
        }
        println("扫描到的配置文件如下")
        keysMap foreach { x =>
            println("配置文件 %s".format(x._1))
            x._2 foreach(y => println("\t%s".format(y)))
        }

        val path = FileSystems.getDefault().getPath(realpath)
//        val dir_watcher = new DirectoryWatcher(path)
        val watch_thread = new Thread(){
            override def run(): Unit = {
                try {
                    val watchService = path.getFileSystem().newWatchService()
                    println("Start Watching...............")
                    path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE)
                    breakable {
                        while (true) {
                            val watchKey = watchService.take()
                            println("Taking one...............")
                            watchKey.pollEvents().asScala.foreach( e => {
                                printEvent(path, e)
                            })
                            if (!watchKey.reset()) {
                                println("No longer valid")
                                watchKey.cancel()
                                watchService.close()
                                break
                            }
                        }
                    }
                } catch {
                    case ie: InterruptedException => println("InterruptedException: " + ie)
                    case ioe: IOException => println("IOException: " + ioe)
                    case e: Exception => println("Exception: " + e)
                }
            }
        }
        watch_thread.start()
    }



    def parse(f : File) : scala.collection.immutable.Map[String, String] = {
        scala.io.Source.fromFile(f, "UTF-8").getLines.map{x =>
            val kv = x.split("=", 2)
            (kv(0).trim, kv(1).trim)
        }.toMap
    }
}
