import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

object Apriori {
  // 两两合并k项频繁项集，取并集得到k+1项频繁项集初步候选集，保证并集个数为k+1，否则返回空串
  def combine(s: String, t: String, k: Int): String = {
    var res = ""
    val slist: Array[String] = s.split(" ")
    val tlist: Array[String] = t.split(" ")
    // 排序保证相同项集元素顺序一致，例如“1 2”和“2 1”是相同项集
    val stlist: Array[String] = slist.union(tlist).distinct.sorted
    if(stlist.length <= k){
      res = stlist.reduce(_+" "+_)
    }
    res
  }

  // 得到某个项集的支持度
  def getSup(t: String, n: Double, data: Array[String]): Double = {
    val cnt: Double = data.count(s => contain(s, t)).toDouble
    cnt / n
  }

  // 判断一个项集s是否包含另一个项集t
  def contain(s: String, t: String): Boolean = {
    // 排序保证相同项集元素顺序一致，例如“1 2”和“2 1”是相同项集
    val slist: Array[String] = s.split(" ").sorted
    val tlist: Array[String] = t.split(" ").sorted
    var i, j = 0
    val slen: Int = slist.length
    val tlen: Int = tlist.length
    while(i < slen && j < tlen){
      // 匹配s和t同时加1比较下一个位置
      if(slist(i) == tlist(j)){
        i = i+1;
        j = j+1;
      }else{
        // 不匹配s位置加1，判断下一个位置能否匹配
        i = i+1;
      }
    }
    // t没有完全匹配
    if(j < tlen){
      return false
    }
    true
  }

  // 判断k项频繁项集是否被k+1项频繁项集中的某一个项集包含
  def check(t: String, nxtFrequent: Array[(String, Double, Int)]): Boolean = {
    val tot: Int = nxtFrequent.count(s => contain(s._1, t))
    if(tot > 0){
      return false
    }
    true
  }

  def main(args: Array[String]): Unit = {
    val input: String = args(0)           //  输入文件
    val output: String = args(1)          // 输出文件
    val sup: Double = args(2).toDouble    // 支持度
    val sparkConf: SparkConf = new SparkConf().setAppName("Apriori")
    val sc = new SparkContext(sparkConf)
    val dataRDD: RDD[String] = sc.textFile(input)
    val data: Array[String] = dataRDD.collect()
    val n: Int = data.length              // 数据集中数据个数（行数）

    // 获得所有空格分隔的项
    val items: RDD[String] = dataRDD.flatMap(x => x.split(" "))
    // 将所有的项去重，结果也是1项频繁项集候选集
    val distinctItems: RDD[String] = items.distinct()
    // k项频繁项集
    val curFrequentItemSet = new ArrayBuffer[(String, Double, Int)]()
    // k项频繁项集极大频繁项集
    val maxFrequentItemSet = new ArrayBuffer[(String, Double, Int)]()
    // 得到不同项个数，确定最大迭代轮数
    val num: Int = distinctItems.count().toInt

    var k = 1
    while(k <= num){
      if(k > 1){
        // k + 1项频繁项集候选集
        var nextFrequentItemSet = new ArrayBuffer[String]()
        for(i <- 0 until curFrequentItemSet.length; j <- i+1 until curFrequentItemSet.length ){
          // k项频繁项集两两取并集，得到个数为k + 1的所有并集
          val combineItem: String = combine(curFrequentItemSet(i)._1, curFrequentItemSet(j)._1, k)
          if(combineItem != ""){
            nextFrequentItemSet += combineItem
          }
        }
        // k + 1项频繁项集初始候选集去重
        nextFrequentItemSet = nextFrequentItemSet.distinct

        // Spark并行化处理，将k + 1项频繁项集候选集中支持度符合条件的选出，得到k + 1项频繁项集
        val combineRDD: RDD[String] = sc.makeRDD(nextFrequentItemSet)
        val frequentRDD: RDD[(String, Double, Int)] = combineRDD.map(t => (t, getSup(t, n.toDouble, data), k)).filter(x => x._2 >= sup)

        // 得到每个k项频繁项集的极大频繁项集
        val resList: Array[(String, Double, Int)] = frequentRDD.collect()
        val maxFrequentItemSetSeg: ArrayBuffer[(String, Double, Int)] = curFrequentItemSet.filter(t => check(t._1, resList))
        maxFrequentItemSetSeg.foreach(maxFrequentItemSet += )

        // 更新k项频繁项集为k + 1项频繁项集，向前迭代
        curFrequentItemSet.clear()
        resList.foreach(curFrequentItemSet +=)
        // 如果k + 1项频繁项集为空，迭代中止退出
        if(curFrequentItemSet.isEmpty){
          k = num
        }
      }else{// k = 1，单独初始化

        // Spark并行化处理，将1项频繁项集候选集中支持度符合条件的选出，得到1项频繁项集
        val frequentRDD: RDD[(String, Double, Int)] = distinctItems.map(t => (t, getSup(t, n.toDouble, data), k)).filter(x => x._2 >= sup)

        // 初始化k项频繁项集为1项频繁项集，向前迭代
        val resList: Array[(String, Double, Int)] = frequentRDD.collect()
        curFrequentItemSet.clear()
        resList.foreach(curFrequentItemSet +=)
        // 如果1项频繁项集为空，迭代中止退出
        if(curFrequentItemSet.isEmpty){
          k = num
        }
      }
      // 向前迭代计算k + 1项频繁项集
      k = k + 1
    }
    // 结果写入文件，为了方便观察输出结果，分区设置为1
    sc.parallelize(maxFrequentItemSet, 1).saveAsTextFile(output)
  }
}
