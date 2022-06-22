package com.cursosdedesarrollo

import java.sql.Date

class Ejemplo03Objeto(
                       var id:Int,
                       var first_name:String,
                       var last_name:String,
                       var created_date:Date,
                       var is_admin:Boolean,
                       var num_points:Int
                     ) {

  override def toString = s"Ejemplo03Objeto(id=$id, first_name=$first_name, last_name=$last_name, created_date=$created_date, is_admin=$is_admin, num_points=$num_points)"
}
