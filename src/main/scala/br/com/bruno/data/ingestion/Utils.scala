package br.com.bruno.data.ingestion

import java.util.regex.Pattern

object Utils {
  def extractRegexGroup(regex: String, string: String, group: Int): String = {
    val pattern = Pattern.compile(regex)
    val matcher = pattern.matcher(string)
    if (matcher.matches) return matcher.group(group)
    null
  }
}
