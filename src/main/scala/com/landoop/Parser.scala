package com.landoop

/*
 * A better alternative to java`s fancy behavior
 *
 * scala> "1,,,,,,2,,,,,,,,,,,".split(",")
 * res1: Array[String] = Array(1, "", "", "", "", "", 2)
 */
object Parser {

  def fromLine(line: String): List[String] = {

    def recursive(lineRemaining: String,
                  isWithinDoubleQuotes: Boolean,
                  valueAccumulator: String,
                  accumulator: List[String]): List[String] = {
      if (lineRemaining.isEmpty)
        valueAccumulator :: accumulator
      else if (lineRemaining.head == '"')
        if (isWithinDoubleQuotes)
          if (lineRemaining.tail.nonEmpty && lineRemaining.tail.head == '"')
          //escaped double quote
            recursive(lineRemaining.drop(2), true, valueAccumulator + '"', accumulator)
          else
          //end of double quote pair (ignore whatever's between here and the next comma)
            recursive(lineRemaining.dropWhile(_ != ','), false, valueAccumulator, accumulator)
        else
        //start of a double quote pair (ignore whatever's in valueAccumulator)
          recursive(lineRemaining.drop(1), true, "", accumulator)
      else if (isWithinDoubleQuotes)
      //scan to next double quote
        recursive(
          lineRemaining.dropWhile(_ != '"')
          , true
          , valueAccumulator + lineRemaining.takeWhile(_ != '"')
          , accumulator
        )
      else if (lineRemaining.head == ',')
      //advance to next field value
        recursive(
          lineRemaining.drop(1)
          , false
          , ""
          , valueAccumulator :: accumulator
        )
      else
      //scan to next double quote or comma
        recursive(
          lineRemaining.dropWhile(char => (char != '"') && (char != ','))
          , false
          , valueAccumulator + lineRemaining.takeWhile(char => (char != '"') && (char != ','))
          , accumulator
        )
    }

    if (line.nonEmpty)
      recursive(line, false, "", Nil).reverse
    else
      Nil
  }

  def fromLines(lines: List[String]): List[List[String]] =
    lines.map(fromLine)
}