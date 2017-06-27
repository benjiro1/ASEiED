import projekt_ase.AvgTempAndWindSpeed
import org.scalatest.{FunSpec, GivenWhenThen}

class JoinJobTest extends FunSpec with GivenWhenThen {

  describe("JoinJobTest") {
    val sessionHolder = new AvgTempAndWindSpeed
    sessionHolder.avgTemp("./src/main/resources/199806daily.txt")
    sessionHolder.avgWindSpeedDaily("./src/main/resources/199806daily.txt")
    sessionHolder.avgWindSpeedHourly("./src/main/resources/199806hourly.txt")
  }

}
