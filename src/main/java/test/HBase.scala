package test
import com.atguigu.streaming.HBaseUtils
import org.apache.hadoop.hbase.TableName

object HBase {
  def main(args: Array[String]): Unit = {

    HBaseUtils.createTable(TableName.valueOf("gs_test05"),"info")
print("123")
    HBaseUtils.deleteTable(TableName.valueOf("gs_test"))
//    val conf = HBaseConfiguration.create
//    conf.set("hbase.zookeeper.quorum","192.168.119.103")
//    conf.set("hbase.zookeeper.property.clientPort","2181")
//
//    val admin = new HBaseAdmin(conf)
//    val descriptor = new HTableDescriptor(TableName.valueOf("gs_test"))
//   val a: TableName = TableName.valueOf("gs_test")
//    descriptor.addFamily(new HColumnDescriptor("info"))
//
//    admin.createTable(descriptor)
//    println("123")





  }
}
