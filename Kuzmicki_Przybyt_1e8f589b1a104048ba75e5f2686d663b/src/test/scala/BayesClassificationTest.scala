package test.scala

import kuzmicki.przybyt.BayesClassification
import org.scalatest.{FunSpec, GivenWhenThen}

class BayesClassificationTest extends FunSpec with GivenWhenThen {

  describe("JoinJobTest") {
    var bayesTest = new BayesClassification()
    bayesTest.classify
  }

}
