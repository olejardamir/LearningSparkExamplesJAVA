/**
 * This is my first Spark program. It is a classical genetic algorithm done by Spark.
 * There is still some code-maintenance that could be done, so this code is not the final version.
 * Everything was put into one class so that user does not need to go between them.
 * However, it is not a clean code, I agree.
 * 
 * @author: Damir Olejar
 * 
 */


package GA;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class GA_Spark_MAIN {

private static Random rnd = new Random();	
private static String alphabet = "GATC";
private static String goal = "GGGAAATTTCCC";
private static String beginFrom = "GATCGTACGTTA";
private static int worldSize = 100;
private static int survivors = 5;	
	
    public static void main(String[] args) {
    	try{
        
		JavaSparkContext sc = new JavaSparkContext("local", "basicmap", System.getenv("SPARK_HOME"), System.getenv("JARS"));
		JavaRDD<String> rdd_world = sc.parallelize(initiateWorld());
		search(rdd_world, sc);
		sc.stop();
    	}catch(Exception e){e.printStackTrace();}

    }	
	
    
   /**
    * Executes the Genetic Algorithm in a loop 
    * @param rdd_world
    * @param sc
    * @throws Exception
    */
	private static void search(JavaRDD<String> rdd_world, JavaSparkContext sc) throws Exception {

		Function2<JavaRDD<String>, JavaSparkContext, String> search = new Function2<JavaRDD<String>, JavaSparkContext, String>() {
			public String call(JavaRDD<String> v1, JavaSparkContext sc) throws Exception {
				boolean end1 = false;
				while (end1 != true) {
					JavaRDD<Tuple2<Integer, String>> mutate = mutateFunction(v1);
					JavaPairRDD<Integer, String> pairs = JavaPairRDD
							.fromJavaRDD(mutate);
					pairs = pairs.sortByKey();
					String check = pairs.first().toString();
					if (check.equals("(0,GGGAAATTTCCC)")) {
						end1 = true;
					} else {
						v1 = sc.parallelize(newWorldAsList(pairs.take(survivors), worldSize));
						System.out.println(StringUtils.join(v1.collect(), ","));
						System.out.println(check);
					}
				}
				System.out.println("DONE!");
				return null;
			}
		};
			search.call(rdd_world, sc);
    }
    
	/**
	 * The mutation method, also an evaluator to generate the key/value pairs
	 * @param rdd
	 * @return
	 */
	private static JavaRDD<Tuple2<Integer, String>> mutateFunction(JavaRDD<String> rdd){	
		
		JavaRDD<Tuple2<Integer, String>> result = rdd.map(new Function<String, Tuple2<Integer, String>>() {
			
			public Tuple2<Integer, String> call(String x) {
				String mutated = mutateIndividual(x);
				Integer key = evaluate(mutated);
				
				return new Tuple2<Integer, String>(key, mutated);
			}
			
			
			private Integer evaluate(String x){
				
				int evaluate = 0;
				for (int t=0;t<x.length();t++){
					if (x.charAt(t)!=goal.charAt(t)){evaluate++;}
				}
				
				return evaluate;
			}
			
			
		    private String mutateIndividual(String individual) {
		        String mutated = "";
		        for (int t = 0; t < individual.length(); t++) {
		            if (rnd.nextInt(2) == 0) {
		                mutated = mutated + getRandomChar();
		            } else {
		                mutated = mutated + individual.charAt(t);
		            }
		        }
		        return mutated;
		    }
			
		    private char getRandomChar() {
		        return alphabet.charAt(rnd.nextInt(alphabet.length()));
		    }
			
		});
		return result;
		
	}
	
	
	
    /**
     * Returns the world as a List
     * @param topN
     * @param popSize
     * @return
     */
    private static List newWorldAsList(List topN, int popSize){
    	int subsize = topN.size();
    	String [] best = new String [subsize];
    	for (int t=0;t<subsize;t++){
    		String element = topN.get(t).toString();
    		best[t] = element.substring(element.indexOf(",")+1, element.indexOf(")"));
    	}
    	
        ArrayList<String> newPopulation = new ArrayList<String>();
        for (int t = 0; t < popSize; t++) {
            newPopulation.add(crossBreed(best));
        }
        return newPopulation;
    }
    

    /**
     * Breeds the best individuals into a new population
     * @param best
     * @return
     */
    private static String crossBreed(String[] best) {
        String parentA = selectParent(best);
        String parentB = selectParent(best);
        String offspring = crossBreedAB(parentA, parentB);
        return offspring;
    }
    
    /**
     * Crosses over any two individuals
     * @param A
     * @param B
     * @return
     */
    private static String crossBreedAB(String A, String B) {
        String offspring = "";
        for (int t = 0; t < A.length(); t++) {
            if (rnd.nextInt(2) == 0) {
                offspring = offspring + A.charAt(t);
            } else {
                offspring = offspring + B.charAt(t);
            }
        }
        return offspring;
    }
    
    
    private static String selectParent(String[] best) {
        return best[rnd.nextInt(best.length)];
    }
    
    

    private static ArrayList<String> initiateWorld() {
    	ArrayList<String> population = new ArrayList();
        for (int t = 0; t < worldSize; t++) {
            population.add(beginFrom);
        }
        return population;

    }
    

    
 
    

	
}
