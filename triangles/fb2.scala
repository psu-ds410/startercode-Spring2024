import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object FB {
    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("FB Exploration") //change this
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    // Return the FB rdd as an RDD of Arrays of Strings
    def getFBArrayRDD(sc: SparkContext): RDD[Array[String]] = {
        val fb = sc.textFile("/datasets/facebook").map{x => x.split(" ")}
        fb
    }

   /***********************************************************************************
    * Question 1: The facebook RDD format is a sequence of numbers separated by spaces
    *   It represents an undirected graph (edges have no arrows).
    *   Our first goal is to determine whether it represents an adjacency list or an 
    *   edge list.
    * 
    * For example, suppose the graph looks like this
    * 1 ---- 2                             
    *   \                                  
    *    \                                 
    *     3                                
    *
    * - An adjacencey list format lists and edge and all of its neighbors, so it would
    *    look like this:
    *    1 2 3  (because 2 and 3 are neighbors of 1)
    *    2 1    (because 1 is a neighbor of 2)
    *    3 1    (because 2 is a neighbor of 1)
    *
    * - A redundant edge list has one line per edge:
    *    1 2
    *    1 3
    *    2 1
    *    3 1
    * - A non-redundant edge list removes lines that mean the same thing. 
    *   For example  1 2  and 2 1 mean the same thing (edge from 1 to 2). 
    *   So in this case, there are many possible representations such as:
    *   1 2
    *   1 3
    *   or
    *   1 2
    *   3 1
    ***********************************************************************************/

   // this code answers whether an RDD is an edge or adjacency list
   // it assumes that the RDD is in one of those formats.
   def isEdgeList(graph: RDD[Array[String]]): Boolean = {
       // The high level strategy is that in an egde list, every row has exactly 2 items
       // while in an adjacency list, there could be 1 or 2 or 3 or ....
       val lengths = graph.map{myarray => myarray.size} // get the length of the array in each row
       val is2 = lengths.filter{mylength => mylength != 2} // check for rows where the length is not 2
       val num_not_2 = is2.count() // count how many rows have lengths not equal to 2
       num_not_2 ==  0 // if there are none, this must be an edge list
       //run through:
       // Suppose the input rdd 'graph' looks like this:
       // Array("1", "2", "3")
       // Array("2", "1")
       // Array("3", "1")
       // Then the 'lengths' rdd looks like this
       // 3
       // 2
       // 2
       // The 'is2' rdd looks like this:
       // 3
       // the variable 'num_not_2' equals 3
       // and we return false because the input was not an edge list

    }
    // we can use the following functions for testing our code as they
    // create an RDD of the same type as the input to isEdgeList
    def adjListForTesting(sc: SparkContext): RDD[Array[String]] = {
        // Create an adjacency list for testing
        val mylist = List(
                           Array("1", "2", "3"),
                           Array("2", "1"),
                           Array("3", "1")
                         )
        sc.parallelize(mylist, 2)
    }

    def redEdgeListForTesting(sc: SparkContext): RDD[Array[String]] = {
        // create a redundant edge list for testing
        val mylist = List(
                       Array("1", "2"),
                       Array("2", "1"),
                       Array("3", "1"),
                       Array("1", "3")
                     )
        sc.parallelize(mylist, 2)
    }

    def nonredEdgeListForTesting(sc: SparkContext): RDD[Array[String]] = {
        // create a nonredundant edge list for testing
        val mylist = List(
                       Array("1", "2"),
                       Array("3", "1")
                     )
        sc.parallelize(mylist, 2)
    }


   /************************************************************************
    * Question 2: suppose we know an RDD is in an egde list format, 
    *  then how can we tell if it is in a redundant or non-redundant format?
    *  note that there can be something in between, like
    *  1 2
    *  1 3
    *  1 2
    *   which is neither (because of the duplicate edge)
    *  Our functions now expected RDD[(String, String)] -- an where every row
    *  is a pair of strings
    ***********************************************************************/
    // returns the Facebook RDD as the edge list type: RDD[(String, String)]
    def getFBEdgeRDD(sc: SparkContext): RDD[(String, String)] = {
        val fb = sc.textFile("/datasets/facebook").map{mystring => mystring.split(" ")}.map{myarray => (myarray(0), myarray(1))}
        fb
    }

    // returns true if the graph is in a reducant edge list format
    def isRedundant(graph: RDD[(String, String)]) = {
      // strategy: if it is redundant, then reversing all the edges gives back the same rdd.
      val reversed = graph.map{case (a,b) => (b, a)} // let's reverse the edges
      // now we need to check whether 'reversed' and 'graph' have the same exact rows
      val common = graph.intersection(reversed) // these are all of the rows they have in common
      // note that 'common' is a subset of 'graph' and also a subset of 'reversed'
      val common_count = common.count()
      val graph_count = graph.count()
      //since 'common' is a subset of 'graph', common_count <= graph_count
      // if common_count == graph_count then this means 'common' has the same rows as 'graph'  
      common_count ==graph_count 
      // this even works if 'graph' has duplicate edges -- in this case we want to return false
      // because duplicate edges are not part of the redundant edge list format.
      // the reason it works correctly when edges are duplicated is because intersection removes duplicates
      //
      // Runthrough #1: 
      //    'graph' looks like 
      //    (1, 2)
      //    (3, 1) 
      //    'reversed' looks like
      //    (2, 1)
      //    (1, 3)
      //    'common' is an empty rdd, so the code correctly returns false
      // Runthrought #2:
      //    'graph' looks like
      //    (1, 2)
      //    (3, 1) 
      //    (2, 1)
      //    (1, 3)
      //    'reversed' looks like
      //    (2, 1)
      //    (1, 3)
      //    (1, 2)
      //    (3, 1) 
      //    'common' looks like:
      //    (2, 1)
      //    (1, 3)
      //    (1, 2)
      //    (3, 1) 
      //    so the code will return True
      // Runthough #3                              
      //    'graph' looks like, not repeated (1, 2) and repeated (2, 1). Because of repetition, we want false to be returned
      //    (1, 2)
      //    (3, 1) 
      //    (2, 1)
      //    (1, 3)
      //    (1, 2)
      //    (2, 1)
      //    'reversed' looks like
      //    (2, 1)
      //    (1, 3)
      //    (1, 2)
      //    (3, 1) 
      //    (2, 1)
      //    (1, 2)
      //    'common' looks like (duplicates in the interesection are automatically removed by .intersection()
      //    (2, 1)
      //    (1, 3)
      //    (1, 2)
      //    (3, 1) 
      //    so common_count will be 4 but graph_count will be 6, so code correctly returns false.
                                    
    }
    // code for testing the function above:
    def toyRedundant(sc: SparkContext): RDD[(String, String)] = {
        // create a redundant edge list for testing
        val mylist = List(
                       ("1", "2"),
                       ("2", "1"),
                       ("3", "1"),
                       ("1", "3")
                     )
        sc.parallelize(mylist, 2)
    }

    def toyNonRedundant(sc: SparkContext): RDD[(String, String)] = {
        // create a nonredundant edge list for testing
        val mylist = List(
                       ("1", "2"),
                       ("3", "1")
                     )
        sc.parallelize(mylist, 2)
    }
    def toyNeither(sc: SparkContext): RDD[(String, String)] = {
        // Neither redundant nor nonredundant because of duplicate edges
        val mylist = List(
                       ("1", "2"),
                       ("2", "1"),
                       ("3", "1"),
                       ("1", "3"),
                       ("1", "2"),
                       ("2", "1")
                     )
        sc.parallelize(mylist, 2)
    }

   /******************************************************************
    * Question 3: how to take an edge list and make it non-redundant
    *
    * The key idea here is to put edges into a "canonical" form
    *    Since we know (1, 2) and (2, 1) represent the same edge,
    *    we can define the canonical form to be one where the smallest
    *    node is first.
    * So the canonical form of (3, 2) is (2, 3)
    * the canonical form of (3, 5) is (3, 5)
    *
    * Fill in the functions:
    *   makeNonRedundant() <--- use only 1 wide-dependency operation
    *   makeRedundant() <--- hint: use flatMap, also use only 1 wide-dependency operation
    * These functions should work correctly with the test RDDs provided by
    *   toyRedundant()
    *   toyNonRedundant()
    *   toyNeither()
    *
    ******************************************************************/
 
    def makeNonRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        // turn edgeList into nonredundant version
        val canonical = edgeList.map{case (a,b) => if (a < b) { 
                                                       (a, b) 
                                                   } else (b, a)}
        canonical.distinct()
        // If edgeList is this:
        //    (1, 2)
        //    (3, 1) 
        //    (2, 1)
        //    (1, 3)
        //    (3, 2)
        //    (2, 1)
        //  Canonical is this:
        //    (1, 2)
        //    (1, 3) 
        //    (1, 2)
        //    (1, 3)
        //    (2, 3)
        //    (1, 2)
        // Want output to look like this: 
        //    (1, 2)
        //    (1, 3) 
        //    (2, 3)
    } 

    def makeRedundant(edgeList: RDD[(String, String)]): RDD[(String, String)] = {
        // first we use flatMap to ensure each edge and its mirror is in the result
        val myrdd = edgeList.flatMap{case (a,b) => List( (b,a) , (a,b) )}
        myrdd.distinct() // the only thing left was to deal with duplicates
        // If edgeList is this:
        //    (1, 2)
        //    (3, 1) 
        //    (1, 2)
        //    (3, 2)
        // myrdd
        // (2, 1) -> created from the (1, 2) in edgeList
        // (1, 2) -> created from the (1,2) in edgeLIst
        // (1, 3)
        // (3, 1)
        // (1, 2)
        // (2, 1)
        // (3, 2)
        // (2, 3)
        // myresult
        // (2, 1) 
        // (1, 2) 
        // (1, 3)
        // (3, 1)
        // (3, 2)
        // (2, 3)
    }


     /****************************************************************************************
      * Q4: now that we have practice using canonical representations,
      *   can we use them to write better algorithms to check for redundant and nonredundant 
      *   edge lists that work properly with toyRedundnat(), toyNonRedundant(), toyNeither()?
      *
      *   isRedundant() used 2 actions and 1 wide dependency transformation
      *
      * Fill in the following functions:
      *    betterIsNonRedundant()   <-- use aggregateByKey (wide dependency transformation),
      *                                    canonical representation, and an (action)
      *    betterIsRedundant()   <-- use aggregateByKey (wide dependency transformation),
      *                                    canonical representation, and reduce (action)
      *
      ************************************************************************************************/
      
     def betterIsNonRedundant(edgeList: RDD[(String, String)]) = {
        // return True if edgeList is not redudant
        // use canonical representations, aggregateByKey, and reduce
        // strategy: for every canonical representation, count how many edges appear
        val canonical = edgeList.map{case (a,b) => if(a<b) {   //create canonical form
                                                      (a,b)
                                                   } else {(b,a)}}
        val kv = canonical.map{case (a,b) => ((a,b), 1)} //the edge is the key, the value is 1 so we can count
        val aggregated = kv.reduceByKey{case (runningsum, nextel) => runningsum + nextel}
        val notone = aggregated.filter{case ((a,b), v) => v != 1}
        val numBadRows = notone.count() // if this is 0, we had a nonredundant graph, otherwise not
        numBadRows == 0
        // If edgeList is this:
        //    (1, 2)
        //    (3, 1) 
        //    (1, 2)
        //    (3, 2)
        //    (2, 3)
        // canonical will look like this
        //    (1, 2)
        //    (1, 3) 
        //    (1, 2)
        //    (2, 3)
        //    (2, 3)
        // kv will look like this
        //    ((1, 2),  1)
        //    ((1, 3),  1) 
        //    ((1, 2),  1)
        //    ((2, 3),  1)
        //    ((2, 3),  1)
        // aggregated will look like this
        //    ((1, 2),  2)
        //    ((1, 3),  1) 
        //    ((2, 3),  2)
        // notone will look like this
        //    ((1, 2),  2)
        //    ((2, 3),  2)
        //We want to eventually get something like this:
        // ((1,2),   2) <- edge (1,2) appears twice
        // ((1,3),   1) <- there is one occurence of either 1,3 or 3,1
        // ((2,3),   2) <- edges equivalent to (2,3) appear twice (i.e., (2,3) and (3,2)) 
     }

     def betterIsRedundant(edgeList: RDD[(String, String)]) = {
        // return True if edgeList is redudant
        // use canonical representations, aggregateByKey, and one action
      
        // canonical form of the edge because a key, and the value is a tuple (x,y)
        //    x represents whether we inverted edge order when making canonical form (1 no, -1 yes)
        //    y represents 1 (we use this to count how many times an edge or its mirror occurred)
        val canonical = edgeList.map{case (a,b) => if(a<b) {   //create canonical form
                                                      ((a,b), (1, 1))
                                                   } else {
                                                      ((b,a), (-1, 1))
                                                   }}
        // when we aggregateByKey
       // we will have something like this (2,3)   values= [(-1, 1), (1, 1), (-1, 1)]
       // if all the x values add up to 0, we know (2,3) appeared as often as (3,2)
       // if we add up the y values, then we know how many times this edge is mentioned
       // so if sum of the x's is 0 and sum of the y's is 2, then this is good (2,3) and (3,2) appeared just once
       val aggregated = canonical.aggregateByKey((0,0))({
                        case ((runningX, runningY),  (x,y)) => (runningX + x , runningY +y)
                    }, 
                    {
                        case ((runningX, runningY),  (x,y)) => (runningX + x , runningY +y)
                    })

        val badrows = aggregated.filter{case ((a,b), (x,y)) => x != 0 || y != 2}  // get rid of good rows where value is (0, 2)
        badrows.count() == 0
        // If edgeList is this:
        //    (1, 2)
        //    (3, 1) 
        //    (1, 2)
        //    (3, 2)
        //    (2, 3)
        // canonical
        //    (1, 2), (1, 1)
        //    (1, 3), (-1, 1)
        //    (1, 2), (1, 1)
        //    (2, 3), (-1, 1)   <- the -1 tells us the edge was (3, 2) originally
        //    (2, 3), (1, 1)   <- the first 1 tells us the edge was (2, 3)
        // aggregated: 
        // (1, 2), (2, 2)  <--- not good, 2 versions of this edge appear, but they are both (1,2), so mirror is absent
        // (1, 3), (-1, 1) <--- not good, only 1 version of the edge appears and it is (3,1)
        // (2,3),  (0, 2)  <--- good, no repetitions, (2,3) and (3,2) appear equal number of times
     }


    /*****************************************************************
     *  Q5: given an edge list, count the number of triangles.
     *    use toyGraph() below as a testing dataset
     *
     *  fill in countTriangles()
     *
     *****************************************************************/

   def toyGraph(sc: SparkContext): RDD[(String, String)] = {
       // create a toy graph for triangle counting
       //
       // 1 ----- 2
       // | \     |
       // |   \   |
       // |     \ |
       // 4-------3 ------ 5
       val mylist = List[(String, String)](
                         ("1", "2"),
                         ("2", "1"),
                         ("2", "3"),
                         ("3", "2"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3"),
                         ("3", "4"),
                         ("3", "5"),
                         ("5", "3"),
                         // add some tricky things
                         ("1", "3"), // duplicate
                         ("3", "1"),
                         ("1", "1"),  //self edge
                         ("3", "5"),
                         ("5", "3"),
                         ("1", "3"),
                         ("3", "1"),
                         ("1", "4"),
                         ("4", "1"),
                         ("4", "3")
                        )
        sc.parallelize(mylist, 2)
    }

    def countTriangles(edgeList: RDD[(String, String)]) = {
        // make sure you are working with a redundant edge list
        // first create an rdd 'paths' where each row represents a path of length 2 in the graph
        // then use joins (may be tricky) to check which paths a-b-c of length 2 have the edge
        // (a,c) in the graph (this would complete the triangle).
    }


}
                                                                                                                          


