package fr.s3ni0r.utils.logger

case class Logger(name: String) extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(name)
}
