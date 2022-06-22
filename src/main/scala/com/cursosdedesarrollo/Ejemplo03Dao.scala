package com.cursosdedesarrollo

import java.sql.{Connection, PreparedStatement, Statement}

class Ejemplo03Dao(var connection: Connection) {


  def createTable: Boolean ={
    val createTableSQL="""create table users (
                             id int unsigned auto_increment not null,
                             first_name varchar(32) not null,
                             last_name varchar(32) not null,
                             date_created timestamp default now(),
                             is_admin boolean,
                             num_points int,
                             primary key (id)
                         );"""
    val statement=connection.prepareStatement(createTableSQL);
    try{
      statement.execute()
    } catch {
      case e: Exception => {
        false
      }
    }
  }
  def save(objeto:Ejemplo03Objeto):Ejemplo03Objeto ={
    // the mysql insert statement// the mysql insert statement

    val query: String = " insert into users (first_name, last_name, date_created, is_admin, num_points)" +
      " values (?, ?, ?, ?, ?)"

    // create the mysql insert preparedstatement
    val preparedStmt: PreparedStatement = connection.prepareStatement(query,Statement.RETURN_GENERATED_KEYS)
    preparedStmt.setString(1, objeto.first_name)
    preparedStmt.setString(2, objeto.last_name)
    preparedStmt.setDate(3, objeto.created_date)
    preparedStmt.setBoolean(4, objeto.is_admin)
    preparedStmt.setInt(5, objeto.num_points)

    // execute the preparedstatement

    val res = preparedStmt.executeUpdate()
    val rs = preparedStmt.getGeneratedKeys
    var id = 0
    if (rs.next) id = rs.getInt(1)
    objeto.id=id
    objeto
  }
  def findAll:List[Ejemplo03Objeto] ={
    val selectSQL="select * from users;"
    val statement=connection.prepareStatement(selectSQL);
    val rs=statement.executeQuery()

    var listado= List[Ejemplo03Objeto]()
    // iterate through the java resultset// iterate through the java resultset
    while ( {
      rs.next
    }) {
      val id = rs.getInt("id")
      val firstName = rs.getString("first_name")
      val lastName = rs.getString("last_name")
      val dateCreated = rs.getDate("date_created")
      val isAdmin = rs.getBoolean("is_admin")
      val numPoints = rs.getInt("num_points")
      // print the results
      //System.out.format("%s, %s, %s, %s, %s, %s\n", id, firstName, lastName, dateCreated, isAdmin, numPoints)
      listado = listado ::: List(new Ejemplo03Objeto(id,firstName,lastName,dateCreated,isAdmin,numPoints))
    }
    listado
  }
  def findByID(id:Int):Ejemplo03Objeto={
    val selectSQL="select * from users where id=?;"
    val statement=connection.prepareStatement(selectSQL);
    statement.setInt(1, id)
    val rs=statement.executeQuery()

    var objeto:Ejemplo03Objeto=null
    // iterate through the java resultset// iterate through the java resultset
    if ( {
      rs.next
    }) {
      val id = rs.getInt("id")
      val firstName = rs.getString("first_name")
      val lastName = rs.getString("last_name")
      val dateCreated = rs.getDate("date_created")
      val isAdmin = rs.getBoolean("is_admin")
      val numPoints = rs.getInt("num_points")
      // print the results
      //System.out.format("%s, %s, %s, %s, %s, %s\n", id, firstName, lastName, dateCreated, isAdmin, numPoints)
      objeto=new Ejemplo03Objeto(id,firstName,lastName,dateCreated,isAdmin,numPoints)
    }
    objeto
  }
  def update(objeto:Ejemplo03Objeto):Ejemplo03Objeto ={
    val id=objeto.id
    val query: String = " update users set first_name=?, last_name=?, date_created=?, is_admin=?, num_points=? " +
      " where id=?"

    // create the mysql insert preparedstatement
    val preparedStmt: PreparedStatement = connection.prepareStatement(query,Statement.RETURN_GENERATED_KEYS)
    preparedStmt.setString(1, objeto.first_name)
    preparedStmt.setString(2, objeto.last_name)
    preparedStmt.setDate(3, objeto.created_date)
    preparedStmt.setBoolean(4, objeto.is_admin)
    preparedStmt.setInt(5, objeto.num_points)
    preparedStmt.setInt(6, objeto.id)
    // execute the preparedstatement

    val res = preparedStmt.executeUpdate()
    if (res==null){
      return null
    }
    objeto
  }
  def delete(id:Int):Boolean={

    val query: String = " delete from users  where id=?"
    val preparedStmt: PreparedStatement = connection.prepareStatement(query,Statement.RETURN_GENERATED_KEYS)
    preparedStmt.setInt(1, id)
    val res = preparedStmt.executeUpdate()
    if (res==null){
      return false
    }
    true
  }
}
