package com.xz.spark.base.exercise

import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
import java.util.ArrayList
import java.util.regex.Pattern
import util.control.Breaks._


/**
  * Created by Administrator on 2017-7-24.
  */
object Date {
  def main(args: Array[String]) {
    val empList = Date.getEmpList()
    val deptList = Date.getDeptList()
  }
  val partten = Pattern.compile(",")
  private def empDate(): String ={
    //工号，名字，职务，上司工号，入职日期，薪水，奖金，部门
    val data = new StringBuilder
    data.append("7369,SMITH,CLERK,7902,1980-12-17,800,,20").append("\n")
    data.append("7499,ALLEN,SALESMAN,7698,1981-20-2,1600,300,30").append("\n")
    data.append("7521,WARD,SALESMAN,7698,1981-2-22,1250,500,30").append("\n")
    data.append("7566,JONES,MANAGER,7839,1981-4-2,2975,,20").append("\n")
    data.append("7654,MARTIN,SALESMAN,7698,1981-9-28,1250,1400,30").append("\n")
    data.append("7698,BLAKE,MANAGER,7839,1981-5-1,2850,,30").append("\n")
    data.append("7782,CLARK,MANAGER,7839,1981-6-9,2450,,10").append("\n")
    data.append("7839,KING,PRESIDENT,,1981-11-17,5000,,10").append("\n")
    data.append("7844,TURNER,SALESMAN,7698,1981-9-8,1500,0,30").append("\n")
    data.append("7900,JAMES,CLERK,7698,1981-12-3,950,,30").append("\n")
    data.append("7902,FORD,ANALYST,7566,1981-12-3,3000,,20").append("\n")
    data.append("7934,MILLER,CLERK,7782,1982-1-23,1300,,10")
    data.toString()
  }
  private def deptDate(): String ={
    //工号，名字，职务，上司工号，入职日期，薪水，奖金，部门
    val data = new StringBuilder
    data.append("10,ACCOUNTING,NEW YORK").append("\n")
    data.append("20,RESEARCH,DALLAS").append("\n")
    data.append("30,SALES,CHICAGO").append("\n")
    data.append("40,OPERATIONS,BOSTON").append("\n")
    data.toString()
  }
  def getEmpList(): ArrayList[Emp] ={
    val emp = empDate()
    val input = emp.getBytes("utf-8")
    val bis = new ByteArrayInputStream(input)
    val isr = new InputStreamReader(bis)
    val br = new BufferedReader(isr)
    var line:String = ""
    val array = new ArrayList[Emp]
    while (line != null  ){
      line=br.readLine()
      breakable {
        if(line==null || line.trim.equals("")){
          break
        }
        val items = Date.partten.split(line)
        if (items.length<8){
          break
        }
        val emp = new Emp(items)
        array.add(emp)
      }
    }
    br.close()
    isr.close()
    bis.close()
    array
  }
  def getDeptList(): ArrayList[Dept] ={
    val emp = empDate()
    val input = emp.getBytes("utf-8")
    val bis = new ByteArrayInputStream(input)
    val isr = new InputStreamReader(bis)
    val br = new BufferedReader(isr)
    var line = ""
    val array = new ArrayList[Dept]

    while (line!=null  ){
      line=br.readLine()
      breakable {
          if(line==null || line.trim.equals("")){
            break()
          }
          val items = Date.partten.split(line)
          if (items.length<3){
            break()
          }
          val dept = new Dept(items)
          array.add(dept)
        }
    }
    br.close()
    isr.close()
    bis.close()
    array
  }
  //求各个部门的总工资
  //求各个部门的人数和平均工资
  //求每个部门最早进入公司的员工姓名
  //求各个城市的员工的总工资
  //列出工资比上司高的员工姓名及其工资
  //列出工资比公司平均工资要高的员工姓名及其工资
  //列出名字以J开头的员工姓名及其所属部门名称
  //列出工资最高的头三名员工姓名及其工资
}
class Emp(items:Array[String]){

  private val id = items(0)
  private val name = items(1)
  private val workName = items(2)
  private val pid = items(3)
  private val workDay = items(4)
  private val sale1 = items(5)
  private val sale2 = items(6)
  private val deptno = items(7)

  def getId(): Int ={
    Integer.parseInt(id)
  }
  def getName(): String={
    name
  }
  def getWorkName(): String={
    workName
  }
  def getPid(): Int ={
    Integer.parseInt(pid)
  }
  def getWorkDay(): String ={
    workDay
  }
  def getSale1(): Int ={
    Integer.parseInt(sale1)
  }
  def getSale2(): Int ={
    Integer.parseInt(sale2)
  }
  def getDeptno(): Int ={
    Integer.parseInt(deptno)
  }
  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("id-").append(id).append("\n");
    sb.append("name-").append(name).append("\n");
    sb.append("workName-").append(workName).append("\n");
    sb.append("pid-").append(pid).append("\n");
    sb.append("workDay-").append(workDay).append("\n");
    sb.append("sale1-").append(sale1).append("\n");
    sb.append("sale2-").append(sale2).append("\n");
    sb.append("deptno-").append(deptno).append("\n");
    sb.toString()
  }
}

class Dept(items:Array[String]){
  private val deptno = items(0)
  private val desc = items(1)
  private val location = items(2)

  def getDeptno(): Int ={
    Integer.parseInt(deptno)
  }
  def getDesc(): String={
    desc
  }
  def getLocation(): String={
    location
  }

  override def toString: String = {
    val sb = new StringBuilder()
    sb.append("deptno-").append(deptno).append("\n")
    sb.append("desc-").append(desc).append("\n")
    sb.append("location-").append(location).append("\n")
    sb.toString()
  }
}
