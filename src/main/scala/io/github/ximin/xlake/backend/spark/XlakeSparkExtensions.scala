package io.github.ximin.xlake.backend.spark

import org.apache.spark.sql.SparkSessionExtensions

class XlakeSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPostHocResolutionRule { session =>
      new RangeRoutingRule(session)
    }
    extensions.injectQueryPostPlannerStrategyRule { session =>
      new PreferredLocationRule(session)
    }
  }
}