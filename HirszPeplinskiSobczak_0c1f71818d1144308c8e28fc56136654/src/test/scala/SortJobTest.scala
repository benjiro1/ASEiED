import com.jwszol.SortJob
import org.scalatest.{FunSpec, GivenWhenThen}

/**
  * Created by jwszol on 12/06/17.
  */
class SortJobTest extends FunSpec with GivenWhenThen {

  describe("JoinJobTest") {
    val jj = new SortJob
    jj.joinData
    jj.quickSortAll
  }

}
