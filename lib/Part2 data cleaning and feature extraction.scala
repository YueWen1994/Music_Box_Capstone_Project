//this script cleans data from the 15.8 g contat-paly dataframe and extract features from the dataset.
// author: Yue Wen
gcloud auth login
gcloud config set compute/zone us-east1-d
gcloud config set project music-box-18185
//wget  --user demo --password demo4wsa  https://wsadb3.wsainc.com/clients/demo/CU/CMBSDealMaster_20170911_AAAAAFtpE3U-.txt  -O ~/file.txt
//wget  --user demo --password demo4wsa  https://wsadb3.wsainc.com/clients/demo/CU/CMBSLoanPerf_20170911_AAAAAFtz7Cs-.txt  -O ~/loan.txt
// console, sotrage, click  the  bucket and then uplpad file there

spark-shell

val text = sc.textFile("gs://musicbox_play/concat_play.csv") 
//count 161244212
val data = text.map(l => l.split(','))
// set schema
val index = 0
val uid = 1
val device = 2
val song_id = 3
val song_type = 4
val song_name = 5
val singer = 6
val play_time = 7
val song_length = 8
val paid_flag = 9
val file_name = 10 

// clean data
val data_noh = data.mapPartitionsWithIndex{ (idx,iter) => if (idx == 0) iter.drop(1) else iter }

//remove index,song name and singer
val clean_data_noh = data_noh.filter( p => p.length ==11)
// remove some data with wrong format
val clean_data_noh2 = clean_data_noh.filter( p => p(10).length ==19)
//only keep useful columns
val trunc_data = clean_data_noh2.map( p =>Array(p(uid),p(device),p(song_type),p(play_time),p(song_length),p(file_name)))
//159865873 
val clean_data = trunc_data.filter( p => p(0)!="" && p(1)!=""&& p(2)!="" && p(3)!=""&& p(4)!="" && p(5)!="" )
//159348176
//further truncate unuseful columns.
val clean_data2 = clean_data.map(p => Array(p(0),p(1),p(2),p(3),p(4),p(5).slice(4,8)))
//remove features with wrong format
val clean_data3 = clean_data2.filter(f => f(2).slice(0,1).forall(_.isDigit) && f(2).takeRight(1).forall(_.isDigit))

def detect_non_numeric_str(s: String): Double = { try{  s.toDouble  } catch{case e: Exception => -2.0}}
//remove song length/playlegth that is not numerical str 
val clean_data4 = clean_data3.filter(f => detect_non_numeric_str(f(4)) > -2.0)
val clean_data5 = clean_data4.filter(f => detect_non_numeric_str(f(3)) > -2.0)
//change file name to the number of days to futher build features in differnt time window
def the_n_th_day(s: String): Double={ if (s.slice(0,2) == "03") (s.slice(2,4).toDouble - 28) else if (s.slice(0,2) == "04") (s.slice(2,4).toDouble + 3) else (s.slice(2,4).toDouble + 33)}
// truncate the dataset from day 28
val clean_data6 = clean_data5.filter(f => the_n_th_day(f(5)) >= 0)

// save the stage 1 data file
clean_data6.map(_.mkString(",")).coalesce(1).saveAsTextFile("gs://musicbox_play/clean_data.csv")

/// extract feature 

//device
val deviceRDD = clean_data6.map(f => Array(f(0),f(1)))
val deviceDF = deviceRDD.toDF()
val device_unique = deviceDF.dropDuplicates()
device_unique.rdd.map(_.mkString(",")).coalesce(1).saveAsTextFile("gs://musicbox_play/device.csv")
// last 8 days cnt
val last_n_days = clean_data6.filter( f => the_n_th_day(f(5)) >= 35)
val temp = last_n_days.map(f => (f(0),1))
val last_n_days_cnt = temp.reduceByKey( (a,b) => a+b)

last_n_days_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/last_n_days_cnt.csv")
//0 - 11 days cnt
val p1_days = clean_data6.filter( f => the_n_th_day(f(5)) < 12 )
val temp_p1 = p1_days.map(f => (f(0),1))
val p1_cnt = temp_p1.reduceByKey( (a,b) => a+b)
p1_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/p1_cnt.csv")
//12 - 23 days cnt
val p2_days = clean_data6.filter( f => the_n_th_day(f(5)) >= 12 && the_n_th_day(f(5)) < 24 )
val temp_p2 = p2_days.map(f => (f(0),1))
val p2_cnt = temp_p2.reduceByKey( (a,b) => a+b)
p2_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/p2_cnt.csv")
//24-36 days cnt
val p3_days = clean_data6.filter( f => the_n_th_day(f(5)) >= 24 && the_n_th_day(f(5)) < 36 )
val temp_p3 = p3_days.map(f => (f(0),1))
val p3_cnt = temp_p3.reduceByKey( (a,b) => a+b)
p3_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/p3_cnt.csv")

//uid, device, songtype, playlen, songlen, date

val songtype = clean_data6.map(f => Array(f(0),detect_non_numeric_str(f(2))))
val type0 = songtype.filter(f => f(1) ==0.0)
val type1 = songtype.filter(f => f(1) ==1.0)
val type2 = songtype.filter(f => f(1) ==2.0)
val type3 = songtype.filter(f => f(1) ==3.0)

val type0_temp = type0.map(f => (f(0),1))
val type1_temp = type1.map(f => (f(0),1))
val type2_temp = type2.map(f => (f(0),1))
val type3_temp = type3.map(f => (f(0),1))

val type0_cnt = type0_temp.reduceByKey((a,b) => a+b)
val type1_cnt = type1_temp.reduceByKey((a,b) => a+b)
val type2_cnt = type2_temp.reduceByKey((a,b) => a+b)
val type3_cnt = type3_temp.reduceByKey((a,b) => a+b)

type0_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/type0_cnt.csv")
type1_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/type1_cnt.csv")
type2_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/type2_cnt.csv")
type3_cnt.coalesce(1).saveAsTextFile("gs://musicbox_play/type3_cnt.csv")

// percentage of song listened

val unique_user = trunc_data.map( p => p(0)).distinct
// 807930 

val sample_perc = 0.1

val sample_user = sc.broadcast(unique_user.sample(false, sample_perc, 123).collect.toSet)

val sample_data = clean_data.filter( f=> sample_user.value.contains(f(0)))
//16834589 

sample_data.map(_.mkString(",")).coalesce(1).saveAsTextFile("gs://musicbox_play/sample_data2.csv")