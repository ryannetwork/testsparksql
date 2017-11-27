package Config

import com.typesafe.config.ConfigFactory


object Settings {
  private val config = ConfigFactory.load()

  object WebLogGen {
    private val weblogGen = config.getConfig("memberstream")

    lazy val records = weblogGen.getInt("records")
    lazy val filePath = weblogGen.getString("file_path")
    lazy val destPath = weblogGen.getString("dest_path")
    lazy val numberOfFiles = weblogGen.getInt("number_of_files")

  }
}
