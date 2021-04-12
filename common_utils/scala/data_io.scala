 /* This function write the input dataframe to the output file system*/
  def WriteDataToDisk(df:DataFrame,outputPath:String,fileName:String): Unit ={
    df.coalesce(1).write.mode(SaveMode.Overwrite)
      .option("header","true")
      .option("mapreduce.fileoutputcommitter.marksuccessfuljobs","false") //Avoid creating of crc files
      .option("encoding", "UTF-8")
      .option("delimiter", outputCsvDelimiter) // set tab as delimiter, required by tranSMART
      .csv(outputPath+"/"+fileName)
  }
