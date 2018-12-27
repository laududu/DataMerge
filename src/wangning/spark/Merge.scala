package wangning.spark
import java.security.MessageDigest
import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, Minutes}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

object Merge {
  def md5(s : String) : String = {
    val hash = MessageDigest.getInstance("MD5").digest(s.getBytes)
    hash.map("%02x".format(_)).mkString
  }

  def checkData(num1: String, num2: String): Boolean = {
    if (StringUtils.isBlank(num1) || StringUtils.isBlank(num2) || "0".equals(num1) || "0".equals(num2)) {
      false
    } else {
      true
    }
  }

  def main(args: Array[String]): Unit = {
    val dateString = args(0)
    val dateArray = dateString.split(",")
    val srcFieldStr = "datasource,carrier_id,call_type,busi_type,start_time,serv_num,serv_ori_num,serv_imsi,serv_imei,serv_homezip,serv_citycode,serv_visitzip,serv_visit_citycode,msc_id,serv_lac,serv_ci,serv_end_lac,serv_end_ci,other_num,other_ori_num,other_homezip,other_citycode,duration,connect_flag,third_num,serv_lon,serv_lat,serv_lon_lat,serv_end_lon,serv_end_lat,serv_end_lon_lat,content,dtmf,roam_carrier_id,serv_tmsi,serv_old_tmsi,alter_time,answer_time,end_time,bsc_point_code,msc_point_code,other_visit_msc,other_lac,other_ci,ss_flag,cf_type,total_sms_num,cur_num,sms_ref"

    var descField = "send_carrier_id,send_num,send_imsi,send_imei,send_homezip,send_visitzip,send_lac,send_ci,send_lon,send_lat,send_lon_lat,send_tmsi,send_old_tmsi,rece_carrier_id,rece_num,rece_imsi,rece_imei,rece_homezip,rece_visitzip,rece_lac,rece_ci,rece_lon,rece_lat,rece_lon_lat,rece_tmsi,rece_old_tmsi,call_type,start_time,msc_id,content,bsc_point_code,msc_point_code,total_sms_num,cur_num,sms_ref"

    val srcFields = srcFieldStr.split(",")
    var outputField = descField.split(",")
    val appName = "SendRecvAndLongShortMerge-" + dateString
    val spark = SparkSession.builder.appName(appName).getOrCreate()

    for (i <- dateArray){
      var date = i.toString
      val inputpath= "hdfs://194.169.1.92:8020/data/DW/SMDR_PARQUET/" + date +"temp/carrier=*/"
      val outputpath = "hdfs://194.169.1.92:8020/data/DW/merge/"+ date
      val errnumpath = "hdfs://194.169.1.92:8020/data/DW/merge_err/"+date
      val paquetDf = spark.read.format("parquet").load(inputpath)
      val parquetRd = paquetDf.rdd
      val data2 = parquetRd.map(line => {
        val ss = line.toString.split(",")
        var a = 0
        val arr = new Array[String](49)
        arr(0) = ss(2)
        arr(1) = ss(3)
        arr(2) = ss(11)
        arr(3) = ss(4)+"4g"
        for( a <- 4 to 48 ){
          arr(a) = ss(a+8)
        }
        arr.mkString(",")
      })
      val data1 = data2.map(_.replaceAll("null",""))
      val data = data1.distinct().coalesce(240).map(line => StringUtils.splitPreserveAllTokens(line,","))
      data.cache
      data.count
      val serv_num_seq = 5
      val other_num_seq = 18
      val content_seq = 31
      val call_type_seq = 2
      val goodstringdata = data.filter(fields => checkData(fields(serv_num_seq), fields(other_num_seq)))
      val badstringdata = data.filter(fields => !checkData(fields(serv_num_seq), fields(other_num_seq)))

      val md5data = goodstringdata.map(fields => {
        val domainArray = new Array[String](3)
        if ("49".equals(fields(call_type_seq))) {
          domainArray(0) = fields(serv_num_seq)
          domainArray(1) = fields(other_num_seq)
          domainArray(2) = fields(content_seq)
        } else {
          domainArray(0) = fields(other_num_seq)
          domainArray(1) = fields(serv_num_seq)
          domainArray(2) = fields(content_seq)
        }
        domainArray.mkString(",")
      })
      val bigCntNum = md5data.map(line => (line, 1)).reduceByKey(_ + _).map(t=> (t._2, t._1)).filter(f=> f._1>360).map(t=>{
        val domainarray = t._2.split(",")
        domainarray(0)
      })
      bigCntNum.saveAsTextFile(errnumpath)
      val errNum = bigCntNum.collect().toSet

      val norData1 = goodstringdata.filter(t => !(t(2)=="49" && errNum.contains(t(serv_num_seq))))
      val norData = norData1.filter(t => !(t(2)=="50" && errNum.contains(t(other_num_seq))))

      val mapnorData = norData.map(fields => {
        val srcMap = new mutable.HashMap[String, String]()
        for (index <- fields.indices) {
          srcMap += (srcFields(index)->fields(index))
        }
        srcMap
      })

      val mapbadData = badstringdata.map(fields => {
        val srcMap = new mutable.HashMap[String, String]()
        for (index <- fields.indices) {
          srcMap += (srcFields(index)->fields(index))
        }
        srcMap
      })

      val groupdata = mapnorData.groupBy(fields => {
        val domainArray = new Array[String](3)
        if ("49".equals(fields("call_type"))) {
          domainArray(0) = fields("serv_num")
          domainArray(1) = fields("other_num")
          domainArray(2) = fields("content")
        } else {
          domainArray(0) = fields("other_num")
          domainArray(1) = fields("serv_num")
          domainArray(2) = fields("content")
        }
        md5(domainArray.mkString(","))
      })

      val fillgooddata = groupdata.map(t => {
        val recordMap = new mutable.HashMap[String, String]()
        val iter = t._2.iterator
        var start_time = "3"
        var byMoOrMt = "-1"
        var rece_l = "-1"
        var rece_c = "-1"
        var send_l = "-1"
        var send_c = "-1"

        while (iter.hasNext) {
          val fields = iter.next()
          if("49".equals(fields("call_type"))) {
            recordMap("send_carrier_id") = fields("carrier_id")
            recordMap("send_num") = fields("serv_num")
            recordMap("send_imsi") = fields("serv_imsi")
            recordMap("send_imei") = fields("serv_imei")
            recordMap("send_homezip") = fields("serv_homezip")
            recordMap("send_visitzip") = fields("serv_visitzip")

            if(send_l=="-1"||((recordMap("send_lac")=="0"||recordMap("send_lac")=="")&&fields("serv_lac")!="0"&&fields("serv_lac")!="")){
              recordMap("send_lac") = fields("serv_lac")
              send_l = "1"
            }

            if(send_c=="-1"||((recordMap("send_ci")=="0"||recordMap("send_ci")=="")&&fields("serv_ci")!="0"&&fields("serv_ci")!="")){
              recordMap("send_ci") = fields("serv_ci")
              send_c = "1"
            }

            recordMap("rece_num") = fields("other_num")
            recordMap("send_lon") = fields("serv_lon")
            recordMap("send_lat") = fields("serv_lat")
            recordMap("send_lon_lat") = fields("serv_lon_lat")
            recordMap("send_tmsi") = fields("serv_tmsi")
            recordMap("send_old_tmsi") = fields("serv_old_tmsi")
            if(rece_l=="-1"||((recordMap("rece_lac")=="0"||recordMap("rece_lac")=="")&&fields("other_lac")!="0"&&fields("other_lac")!="")){
              recordMap("rece_lac") = fields("other_lac")
              rece_l = "1"
            }

            if(rece_c=="-1"||((recordMap("rece_ci")=="0"||recordMap("rece_ci")=="")&&fields("other_ci")!="0"&&fields("other_ci")!="")){
              recordMap("rece_ci") = fields("other_ci")
              rece_c = "1"
            }
          } else {
            recordMap("rece_carrier_id") = fields("carrier_id")
            recordMap("rece_num") = fields("serv_num")
            recordMap("rece_imsi") = fields("serv_imsi")
            recordMap("rece_imei") = fields("serv_imei")
            recordMap("rece_homezip") = fields("serv_homezip")
            recordMap("rece_visitzip") = fields("serv_visitzip")
            if(rece_l=="-1"||((recordMap("rece_lac")=="0"||recordMap("rece_lac")=="")&&fields("serv_lac")!="0"&&fields("serv_lac")!="")){
              recordMap("rece_lac") = fields("serv_lac")
              rece_l = "1"
            }
            if(rece_c=="-1"||((recordMap("rece_ci")=="0"||recordMap("rece_ci")=="")&&fields("serv_ci")!="0"&&fields("serv_ci")!="")){
              recordMap("rece_ci") = fields("serv_ci")
              rece_c = "1"
            }
            recordMap("send_num") = fields("other_num")
            recordMap("rece_lon") = fields("serv_lon")
            recordMap("rece_lat") = fields("serv_lat")
            recordMap("rece_lon_lat") = fields("serv_lon_lat")
            recordMap("rece_tmsi") = fields("serv_tmsi")
            recordMap("rece_old_tmsi") = fields("serv_old_tmsi")

            if(send_l=="-1"||((recordMap("send_lac")=="0"||recordMap("send_lac")=="")&&fields("other_lac")!="0"&&fields("other_lac")!="")){
              recordMap("send_lac") = fields("other_lac")
              send_l = "1"
            }

            if(send_c=="-1"||((recordMap("send_ci")=="0"||recordMap("send_ci")=="")&&fields("other_ci")!="0"&&fields("other_ci")!="")){
              recordMap("send_ci") = fields("other_ci")
              send_c = "1"
            }

          }
          if (byMoOrMt == "-1" || byMoOrMt == fields("call_type")) {
            byMoOrMt = fields("call_type")
          }
          else {
            byMoOrMt = "2"
          }
          recordMap("call_type") = byMoOrMt

          if(start_time > fields("start_time")) {
            start_time = fields("start_time")
            recordMap("start_time") = fields("start_time")
          }
          recordMap("msc_id") = fields("msc_id")
          recordMap("content") = fields("content")
          recordMap("bsc_point_code") = fields("bsc_point_code")
          recordMap("msc_point_code") = fields("msc_point_code")
          recordMap("total_sms_num") = fields("total_sms_num")
          recordMap("cur_num") = fields("cur_num")
          recordMap("sms_ref") = fields("sms_ref")
        }
        recordMap
      }).map(line=>{
        var fieldsarray = new ArrayBuffer[String]()
        for(i <- outputField.indices) {
          fieldsarray += line.getOrElse(outputField(i), "")
        }
        fieldsarray.mkString(",")
      })
      val fillbaddata = mapbadData.map(fields => {
        val recordMap = new mutable.HashMap[String, String]()
        if("49".equals(fields("call_type"))) {
          recordMap("send_carrier_id") = fields("carrier_id")
          recordMap("send_num") = fields("serv_num")
          recordMap("send_imsi") = fields("serv_imsi")
          recordMap("send_imei") = fields("serv_imei")
          recordMap("send_homezip") = fields("serv_homezip")
          recordMap("send_visitzip") = fields("serv_visitzip")
          recordMap("send_lac") = fields("serv_lac")
          recordMap("send_ci") = fields("serv_ci")
          recordMap("rece_num") = fields("other_num")
          recordMap("send_lon") = fields("serv_lon")
          recordMap("send_lat") = fields("serv_lat")
          recordMap("send_lon_lat") = fields("serv_lon_lat")
          recordMap("send_tmsi") = fields("serv_tmsi")
          recordMap("send_old_tmsi") = fields("serv_old_tmsi")
          recordMap("rece_lac") = fields("other_lac")
          recordMap("rece_ci") = fields("other_ci")

        } else {
          recordMap("rece_carrier_id") = fields("carrier_id")
          recordMap("rece_num") = fields("serv_num")
          recordMap("rece_imsi") = fields("serv_imsi")
          recordMap("rece_imei") = fields("serv_imei")
          recordMap("rece_homezip") = fields("serv_homezip")
          recordMap("rece_visitzip") = fields("serv_visitzip")
          recordMap("rece_lac") = fields("serv_lac")
          recordMap("rece_ci") = fields("serv_ci")
          recordMap("send_num") = fields("other_num")
          recordMap("rece_lon") = fields("serv_lon")
          recordMap("rece_lat") = fields("serv_lat")
          recordMap("rece_lon_lat") = fields("serv_lon_lat")
          recordMap("rece_tmsi") = fields("serv_tmsi")
          recordMap("rece_old_tmsi") = fields("serv_old_tmsi")
          recordMap("send_lac") = fields("other_lac")
          recordMap("send_ci") = fields("other_ci")

        }

        recordMap("call_type") = fields("call_type")
        recordMap("start_time") = fields("start_time")
        recordMap("msc_id") = fields("msc_id")
        recordMap("content") = fields("content")
        recordMap("bsc_point_code") = fields("bsc_point_code")
        recordMap("msc_point_code") = fields("msc_point_code")
        recordMap("total_sms_num") = fields("total_sms_num")
        recordMap("cur_num") = fields("cur_num")
        recordMap("sms_ref") = fields("sms_ref")

        recordMap
      }).map(line=>{
        var fieldsarray = new ArrayBuffer[String]()
        for(i <- outputField.indices) {
          fieldsarray += line.getOrElse(outputField(i), "")
        }
        fieldsarray.mkString(",")
      })

      var union_result = fillgooddata.union(fillbaddata)
      union_result.cache
      union_result.count
      union_result.repartition(200)
      val call_type_num2 = 26
      val serv_num_seq2 = 1
      val other_num_seq2 = 14
      val total_num_seq2 = 32
      val start_time_seq2 = 27
      val content_seq2 = 29
      val data_array1 = union_result.map(line => StringUtils.splitPreserveAllTokens(line,","))
      val data_array = data_array1.filter(line => StringUtils.isNotBlank(line(total_num_seq2)) && line(total_num_seq2) != "null")
      val data_multi = data_array.filter(record => record(total_num_seq2).toInt > 1 && ! record(serv_num_seq2).equals("0") && !record(other_num_seq2).equals("0") && StringUtils.isNotBlank(record(serv_num_seq2)) && StringUtils.isNotBlank(record(other_num_seq2)))
      val data_alone = data_array.filter(record => StringUtils.isBlank(record(serv_num_seq2)) || StringUtils.isBlank(record(other_num_seq2)) || record(total_num_seq2).toInt == 1 || record(serv_num_seq2).equals("0") || record(other_num_seq2).equals("0") )
      val data_single = data_alone.map(lineBuffer => {
        lineBuffer.mkString(",") + ",1"
      })
      val data_group = data_multi.groupBy( resultArray => {
        val call_type = resultArray(call_type_num2)
        val serv_num = resultArray(serv_num_seq2)
        val other_num = resultArray(other_num_seq2)
        val total_sms_num = resultArray(total_num_seq2)
        val domainArray = new Array[String](4)
        domainArray(0) = total_sms_num
        domainArray(1) = serv_num
        domainArray(2) = other_num
        domainArray(3) = call_type
        domainArray.mkString(",")
      })
      val df = new SimpleDateFormat("yyyyMMddHHmmss")
      val merge_data = data_group.flatMap(t => {
        val result: ArrayBuffer[String] = new ArrayBuffer[String]()
        val total_sms_num = t._1.split(",")(0).toInt
        val i = t._2.toIterator
        val map :HashMap[Int, scala.collection.mutable.TreeSet[String]] = new HashMap()
        var curnum = 0
        while (i.hasNext) {
          val message = i.next()
          curnum = message( total_num_seq2 + 1).toInt
          map(curnum) = map.getOrElse(curnum, scala.collection.mutable.TreeSet[String]()) + message.mkString(",")  //some problems;
        }
        for (index <- 1 to total_sms_num) {
          val ite = map.getOrElse(index, scala.collection.mutable.TreeSet[String]()).iterator
          while(ite.hasNext) {
            var merge_num = 1
            val message = ite.next()
            val record =  StringUtils.splitPreserveAllTokens(message,",").toBuffer
            val startTime = new DateTime(df.parse(record(start_time_seq2))) //new DateTime(record(0))
            var endTime: DateTime = null
            var isContinue = true
            for(indexx <- index + 1 to total_sms_num) {
              val nextRecordList = map.getOrElse(indexx, scala.collection.mutable.TreeSet[String]())
              if (isContinue && nextRecordList.size > 0) {
                val nextRecord = StringUtils.splitPreserveAllTokens(nextRecordList.head, ",").toBuffer
                endTime = new DateTime(df.parse(nextRecord(start_time_seq2))) //new DateTime(nextRecord(0))
                val diff = Minutes.minutesBetween(startTime, endTime).getMinutes
                if (diff < 1 && diff > -1) {
                  record(content_seq2) = record(content_seq2) + nextRecord(content_seq2)
                  merge_num += 1
                  map(indexx).remove(map(indexx).head)
                } else {
                  isContinue = false
                }
              } else {
                isContinue = false
              }
            }
            //record(total_num_seq2 + 1) = record(total_num_seq2 + 1) + "_" + merge_num.toString
            record.append(merge_num.toString)
            result.append(record.mkString(","))
          }
        }
        result
      })
      val combine_data = merge_data.union(data_single)
      combine_data.cache
      combine_data.count
      combine_data.repartition(200).saveAsTextFile(outputpath)
      combine_data.unpersist()
      union_result.unpersist()
    }
  }
}

