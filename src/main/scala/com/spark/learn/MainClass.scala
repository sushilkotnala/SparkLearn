package com.spark.learn

object MainClass extends App {

  println("Hello")

  // declare val
  val i = 10
  //declare var
  var j = 10
  //expression
  val k = 1 + 2
  //define method
  def calc(i: Int): Int = i + 1
  //define method as expression
  val incr = (i: Int): Int

  // pattern match
  val v = "match"
  val valType = v match {
    case s: String => "String"
    case _ => "Other type"
  }

//  try{
//    throw NullPointerException
//  } catch {
//    case e: NullPointerException => "NullPointerException"
//    case e: Exception => "Exception"
//    case _ => "Unchecked exception"
//  }

  //companion objects
  class Person(name: String, age: Int, status: String)
  object Person{
    def apply(name: String, age: Int): Person = new Person(name, age, "P")
  }


}
