package cn.project.spark.utils

class SecondarySortKey(val first: Double, val second: Double) extends Ordered[SecondarySortKey] with Serializable {
  override def compare(that: SecondarySortKey): Int = {
    if(this.first - that.first != 0) {
      (this.first - that.first).toInt
    } else {
      if(this.second - that.second > 0) {
        (Math.ceil(this.second - that.second)).toInt
      } else if (this.second - that.second < 0) {
        (Math.ceil(this.second - that.second)).toInt
      } else {
        0
      }
    }
  }
}
