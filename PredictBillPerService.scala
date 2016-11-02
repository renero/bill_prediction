// Read traffic_data consumption per call_type

import org.apache.spark.sql.Row

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val t = sqlContext.table("gbic_global.gbic_global_traffic_voice").where('gbic_op_id === 1).
  select($"msisdn_id",$"roaming_rv",$"out_other_rv",$"out_national_onnet_rv",$"out_national_offnet_rv",$"out_national_fixed_rv",$"out_international_rv",$"month")

//t.show(5,false)

val roaming = t.select($"msisdn_id",$"roaming_rv",$"month").rdd.flatMap{
  case Row(id:String, rv:Double, month:String) => Some((id, (idate.monthToNum(month), rv)))
  case _ => None
}
val minMonth = roaming.map(_._2._1).reduce(_ min _)
val users = roaming.map(e => ((e._1, idate.mDif(minMonth, e._2._1)), e._2._2)).
  reduceByKey(_ + _).
  map {
    case ((uid, month), bill) => (uid, (month, bill))
  }.
  groupByKey().
  mapValues(_.toSeq)