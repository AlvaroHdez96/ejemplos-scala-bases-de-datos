package com.cursosdedesarrollo
import org.mongodb.scala.{BulkWriteResult, Document, MongoClient, MongoCollection, MongoDatabase, Observable, Observer}
//filtros como equal
import org.mongodb.scala.model.Filters._
// tipos de ordenación
import org.mongodb.scala.model.Sorts._
//tipos de proyecciones (seleccionando campos)
import org.mongodb.scala.model.Projections._
//tipos de agregación como "filter"
import org.mongodb.scala.model.Aggregates._
// tipos de cambio en campo
import org.mongodb.scala.model.Updates._
// Cambiando Modelos
import org.mongodb.scala.model._
// Cambiando resultados
import org.mongodb.scala.result._

object Ejemplo04Mongo{

  def main(args: Array[String]): Unit = {

    // Conectando a la BBDD
    val client: MongoClient = MongoClient()

    //Obteniendo el listado de BBDD
    val databases=client.listDatabaseNames()
    println("Databases:")
    databases.subscribe(new Observer[String] {
      override def onNext(result: String): Unit = println("Database Name:" +result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Completado")
    })


    // Al ser asíncrono hay que esperar
    Thread.sleep(1000)


  }
}
