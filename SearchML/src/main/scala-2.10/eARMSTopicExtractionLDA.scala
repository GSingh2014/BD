import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.mllib.clustering.{EMLDAOptimizer, OnlineLDAOptimizer, DistributedLDAModel, LDA}
import org.apache.spark.mllib.clustering.LDAModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.clustering.LDA
import java.io.File
import java.text.BreakIterator
import org.apache.spark.mllib.clustering.LDA





/**
 * @author gopasing
 */
object eARMSTopicExtractionLDA {
  
  
    
  def main(args:Array[String]){
    println("testing")
    val conf = new SparkConf(true).setMaster("local[2]").setAppName("eARMs")
    
    val sc = new SparkContext(conf)
    

    
     val dir = new File("/Users/gopasing/eARMS")
    
    val stopwordFile = "/Users/gopasing/eARMS/stopwords.txt"
    
    val dlm = DistributedLDAModel.load(sc, "/Users/gopasing/eARMS/model/trainedLDAmodel")
    val model = dlm.toLocal
    
    val logfiles = dir.listFiles().filter(_.isFile()).filter(f => """.*\.log\..*""".r.findFirstIn(f.getName).isDefined)
    
    var listoffiles = ""
    
    for(f <- logfiles){
      
       val preprocessStart = System.nanoTime()
      
      println("************" + f + "************")
      
      listoffiles = listoffiles + f +","
      
      val textRDD = sc.textFile(f.getPath)
      
     // println("The path is " + f.getPath)
      
            //Tokenize each file
      val tokenizer = new SimpleTokenizer(sc, stopwordFile)
      
      val tokenized: RDD[(Long, IndexedSeq[String])] = textRDD.zipWithIndex().map { case (text, id) =>
                                                          id -> tokenizer.getWords(text)
                                                        }
      tokenized.cache()
      
      println("Printing file " + f.getPath)
      
      //tokenized.foreach(row => println(row._1,row._2 ))
      
      // Counts words: RDD[(word, wordCount)]
      val wordCounts: RDD[(String, Long)] = tokenized
                      .flatMap { case (_, tokens) => tokens.map(_ -> 1L) }
                      .reduceByKey(_ + _)
      wordCounts.cache()
      
      val fullVocabSize = wordCounts.count()
      
      println("fullVocabSize = " + fullVocabSize)
      
      val vocabSize = -1
      
       val (vocab: Map[String, Int], selectedTokenCount: Long) = {
        val tmpSortedWC: Array[(String, Long)] = if (vocabSize == -1 || fullVocabSize <= vocabSize) {
          // Use all terms
          wordCounts.collect().sortBy(-_._2)
        } else {
          // Sort terms to select vocab
          wordCounts.sortBy(_._2, ascending = false).take(vocabSize)
        }
        (tmpSortedWC.map(_._1).zipWithIndex.toMap, tmpSortedWC.map(_._2).sum)
                                                                                }
       // Create the corpus  
        val documents: RDD[(Vector)] = tokenized.map { case (id, tokens) =>
      // Filter tokens by vocabulary, and create word count vector representation of document.
        val wc = new mutable.HashMap[Int, Int]()
        tokens.foreach { term =>
          if (vocab.contains(term)) {
            val termIndex = vocab(term)
            wc(termIndex) = wc.getOrElse(termIndex, 0) + 1
          }
        }
        val indices = wc.keys.toArray.sorted
        val values = indices.map(i => wc(i).toDouble)
  
        val sb = Vectors.sparse(vocab.size, indices, values)
        (sb)
                                  }
        
        //Print the details
        
        documents.cache() 
    
    val actualCorpusSize = documents.count()
    val actualVocabSize = vocab.size
    val preprocessElapsed = (System.nanoTime() - preprocessStart) / 1e9

    println()
    println(s"Corpus summary:")
    println(s"\t Training set size: $actualCorpusSize documents")
    println(s"\t Vocabulary size: $actualVocabSize terms")
    println(s"\t Training set size: $selectedTokenCount tokens")
    println(s"\t Preprocessing time: $preprocessElapsed sec")
    println()
        
    // RUN THE LDA MODEL ON THE CORPUS
    
    val r = model.topicDistributions(documents.zipWithIndex().map(_.swap)).cache()
    
    r.map{case(id,vector) => println("The vector size is = " + vector.size) }
        
    val corpus = documents.zipWithIndex().map(_.swap).cache()
    
    val ldaModel = new LDA().setK(5).run(corpus)
    
    println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        val topics = ldaModel.topicsMatrix
        for (topic <- Range(0, 3)) {
          print("Topic " + topic + ":")
          for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
          println()
}
        
    }
    
  }
}

  private class SimpleTokenizer(sc: SparkContext, stopwordFile: String) extends Serializable {

  private val stopwords: Set[String] = if (stopwordFile.isEmpty) {
    Set.empty[String]
  } else {
    val stopwordText = sc.textFile(stopwordFile).collect()
    stopwordText.flatMap(_.stripMargin.split("\\s+")).toSet
  }

  // Matches sequences of Unicode letters
  private val allWordRegex = "^(\\p{L}*)$".r

  // Ignore words shorter than this length.
  private val minWordLength = 3

  def getWords(text: String): IndexedSeq[String] = {

    val words = new mutable.ArrayBuffer[String]()

    // Use Java BreakIterator to tokenize text into words.
    val wb = BreakIterator.getWordInstance
    wb.setText(text)

    // current,end index start,end of each word
    var current = wb.first()
    var end = wb.next()
    while (end != BreakIterator.DONE) {
      // Convert to lowercase
      val word: String = text.substring(current, end).toLowerCase
      // Remove short words and strings that aren't only letters
      word match {
        case allWordRegex(w) if w.length >= minWordLength && !stopwords.contains(w) =>
          words += w
        case _ =>
      }

      current = end
      try {
        end = wb.next()
      } catch {
        case e: Exception =>
          // Ignore remaining text in line.
          // This is a known bug in BreakIterator (for some Java versions),
          // which fails when it sees certain characters.
          end = BreakIterator.DONE
      }
    }
    words
  }
}