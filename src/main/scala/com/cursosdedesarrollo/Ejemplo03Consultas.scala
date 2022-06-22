package com.cursosdedesarrollo

import java.sql.{Connection, DriverManager}
import java.text.SimpleDateFormat

object Ejemplo03Consultas extends App {

  // connect to the database named "mysql" on port 8889 of localhost
  val url = "jdbc:mysql://localhost:3307/test"
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = "root"
  val password = "root"
  var connection: Connection = null
  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    val dao = new Ejemplo03Dao(connection)


    val meta = connection.getMetaData
    var res = meta.getTables("test", null, "users", null)
    if (res.next()) {
      println("Tabla ya creada")
    } else {
      val res = dao.createTable
      println("Tabla creada?: " + res)
    }


    var fecha = "01/01/1990"
    var formate = new SimpleDateFormat("dd/MM/yyyy").parse(fecha)
    var fechaActual = new java.sql.Date(formate.getTime());
    var objeto = new Ejemplo03Objeto(0, "Pepe", "San", fechaActual, false, 5000)
    objeto = dao.save(objeto)
    var listado = dao.findAll
    println(listado)
    objeto = dao.findByID(objeto.id)
    println(objeto)
    objeto.first_name = "Paco"
    objeto = dao.update(objeto)
    listado = dao.findAll
    println(listado)
    var borrado = dao.delete(objeto.id)
    println("Borrado: " + borrado)
    listado = dao.findAll
    println(listado)

  } catch {
    case e: Exception => e.printStackTrace
  }
  connection.close
}
