import com.szymkru.DailyAvg
import org.scalatest.{FunSpec, GivenWhenThen}


class DailyAvgTest extends FunSpec with GivenWhenThen {

  describe("DailyAvg") {
    val da = new DailyAvg
    da.avg
  }

}
