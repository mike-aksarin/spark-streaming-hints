package data

case class MalformedLine(line: String, error: Option[Throwable] = None)
