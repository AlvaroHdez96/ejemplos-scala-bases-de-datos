package com.cursosdedesarrollo

import org.mongodb.scala.{BulkWriteResult, Completed, Document, MongoClient, MongoCollection, MongoDatabase, Observable, Observer}
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

    //Seleccionando una BBDD
    val database: MongoDatabase = client.getDatabase("mysql")

    var collection: MongoCollection[Document] =
      database.getCollection("test")
    //borrando la base de datos
    collection.drop().subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println("Next")
      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Borrado Collección completado")
    })

    // Creando u Obteniendo la BBDD
    collection= database.getCollection("mycoll")
    // insertando un documento
    val document: Document = Document("x" -> 1)
    var insertObservable: Observable[Completed] = collection.insertOne(document)

    insertObservable.subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println(s"Primer Insert onNext: $result")
      override def onError(e: Throwable): Unit = println(s"onError: $e")
      override def onComplete(): Unit = println("onComplete")
    })

    // insertando otro documento
    val doc: Document = Document(
      "name" -> "MongoDB",
      "type" -> "database",
      "count" -> 1,
      "info" -> Document(
        "x" -> 203,
        "y" -> 102)
    )

    collection.insertOne(doc).subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = {
        println("Next Insert: "+result)
      }

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = {
        println("Insert Complete")
        println("Documento insertado " +doc)
      }
    })
    println("Listado de Documentos")
    var listadoDocumentos:List[Document] = List[Document]()
    // Cogiendo todos los documentos
    collection.find.subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = {
        println(result)
        listadoDocumentos = listadoDocumentos ::: List(result)
      }

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = {
        println("Listado de Documentos Completado")
        //println(listadoDocumentos)
        listadoDocumentos.foreach(println)
      }
    })

    // Cogiendo un documento
    collection.find.first().subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println(result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Find Completado")
    })

    // insertando un montón de documentos
    val documents: IndexedSeq[Document] =
      (1 to 100) map { i: Int => Document("i" -> i) }
    insertObservable = collection.insertMany(documents)

    val insertAndCount = for {
      insertResult <- insertObservable
      countResult <- collection.countDocuments()
    } yield countResult
    insertAndCount.subscribe(new Observer[Long] {
      override def onNext(result: Long): Unit = println(s"Número total de documentos:  $result")

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Inserción múltiple terminada")
    })


    collection.find().first().subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Find Terminado")
    })

    // Query Filters
    // now use a query to get 1 document out
    collection.find(equal("i", 71)).first().subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Find Completado")
    })

    // Filtrado
    collection.find(gt("i", 50)).subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()
      override def onComplete(): Unit = println("Listado Impreso")
    })
    // Filtrado por rango
    collection.find(and(gt("i", 50), lte("i", 100))).subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()
      override def onComplete(): Unit = println("Listado Impreso")
    })

    // Ordenado
    collection.find(exists("i")).sort(descending("i")).first().subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()
      override def onComplete(): Unit = println("Listado Impreso")
    })

    // Proyección
    collection.find().projection(excludeId()).first().subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()
      override def onComplete(): Unit = println("Listado Impreso")
    })

    //Aggregación
    collection.aggregate(Seq(
      filter(gt("i", 0)),
      project(Document("""{ITimes10: {$multiply: ["$i", 10]}}"""))
    )).subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = println("Documento: "+result)
      override def onError(e: Throwable): Unit = e.printStackTrace()
      override def onComplete(): Unit = println("Listado Impreso")
    })

    // Update One
    collection.updateOne(equal("i", 10), set("i", 110)).subscribe(new Observer[UpdateResult] {
      override def onNext(result: UpdateResult): Unit = println("Resultado Update:" +result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Documento Cambiado")
    })

    // Update Many
    collection.updateMany(lt("i", 100), inc("i", 100)).subscribe(new Observer[UpdateResult] {
      override def onNext(result: UpdateResult): Unit = println("Resultado Update:" +result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Documento Cambiado")
    })
    // Delete One
    collection.deleteOne(equal("i", 110)).subscribe(new Observer[DeleteResult] {
      override def onNext(result: DeleteResult): Unit = println("Resultado Delete:" +result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Documento Cambiado")
    })

    // Delete Many
    collection.deleteMany(gte("i", 100)).subscribe(new Observer[DeleteResult] {
      override def onNext(result: DeleteResult): Unit = println("Resultado Delete:" +result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = println("Documento Cambiado")
    })

    collection.drop().subscribe(new Observer[Completed] {
      override def onNext(result: Completed): Unit = println("resultado drop: "+result)

      override def onError(e: Throwable): Unit = e.printStackTrace()

      override def onComplete(): Unit = {
        println("Borrado completado")
        // ordered bulk writes
        val writes: List[WriteModel[_ <: Document]] = List(
          InsertOneModel(Document("_id" -> 4)),
          InsertOneModel(Document("_id" -> 5)),
          InsertOneModel(Document("_id" -> 6)),
          UpdateOneModel(Document("_id" -> 1), set("x", 2)),
          DeleteOneModel(Document("_id" -> 2)),
          ReplaceOneModel(Document("_id" -> 3), Document("_id" -> 3, "x" -> 4))
        )

        collection.bulkWrite(writes).subscribe(new Observer[BulkWriteResult] {
          override def onNext(result: BulkWriteResult): Unit = println("Resultado bulk:" + result)


          override def onError(e: Throwable): Unit = e.printStackTrace()

          override def onComplete(): Unit ={
            println("Bulk completado")
            collection.drop().subscribe(new Observer[Completed] {
              override def onNext(result: Completed): Unit = println("resultado drop: "+result)

              override def onError(e: Throwable): Unit = e.printStackTrace()

              override def onComplete(): Unit = {
                println("Borrado completado")
                collection.bulkWrite(writes, BulkWriteOptions().ordered(false)).subscribe(new Observer[BulkWriteResult] {
                  override def onNext(result: BulkWriteResult): Unit = println("Resultado BulkWrite:" +result)

                  override def onError(e: Throwable): Unit = e.printStackTrace()

                  override def onComplete(): Unit = {
                    println("Bulk Terminado")
                    collection.find().subscribe(new Observer[Document] {
                      override def onNext(result: Document): Unit = println("Documento: "+result)

                      override def onError(e: Throwable): Unit = e.printStackTrace()

                      override def onComplete(): Unit = {
                        println("listado presentado")
                        // Clean up
                        collection.drop().subscribe(new Observer[Completed] {
                          override def onNext(result: Completed): Unit = println("resultado drop: "+result)

                          override def onError(e: Throwable): Unit = {
                            println("Fallo en último drop")
                            e.printStackTrace()
                          }

                          override def onComplete(): Unit = {
                            println("Borrado completado")
                            // Al ser asíncrono hay que esperar
                            Thread.sleep(1000)
                          }
                        })
                      }
                    })

                  }
                })


              }
            })
          }
        })


      }
    })



    // Al ser asíncrono hay que esperar
    Thread.sleep(1000)


  }
}
