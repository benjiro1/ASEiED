import com.project.KMeans
import org.scalatest.{FunSpec, GivenWhenThen}

class KMeansTest extends FunSpec with GivenWhenThen {

  describe("KMeansTest") {
    val km = new KMeans(5)             //data will be grouped in 10 groups
    km.kMeans
  }

}
