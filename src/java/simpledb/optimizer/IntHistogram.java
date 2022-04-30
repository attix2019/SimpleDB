package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int bucketNum;

    private int min;

    private int max;

    private int total;

    // width = Math.ceil( tuplesNumber/ bucketNum)
    private int width;

    // needed if tuplesNumber % bucketNum != 0
    private int lastBinWidth;

    int[] bucketCount;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.bucketNum = buckets;
        this.min = min;
        this.max = max;
        this.total = 0;
        this.width = (int)Math.ceil( (double)(max-min+1)/bucketNum );
        lastBinWidth = (max-min+1) % bucketNum == 0 ? width:(max-min+1) % bucketNum;
        bucketCount = new int[bucketNum];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int dis = v - this.min;
        int pos = dis / width;
        total ++;
        bucketCount[pos]++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double eqfrac = 0;
        double lt = 0;
        double gt = 0;
        if(v < min){
            lt  = 0;
            gt = 1;
        }else if(v > max){
            lt = 1;
            gt = 0;
        }else{
            int dis = v - min;
            int pos = dis/width;
            int w = (pos == bucketNum-1 && (max-min+1) % bucketNum != 0)? this.lastBinWidth: this.width;
            eqfrac =  (double)1/w * bucketCount[pos]/total;

            for(int i = pos + 1; i < bucketNum; i++){
                gt += ((double)bucketCount[i]/total);
            }
            int vLeft = min + width * pos;
            int vRight = min + ((pos == bucketNum - 1 && lastBinWidth != width)? max :width *(pos + 1));
            gt += (vRight - v - 1)*((double)bucketCount[pos]/total);

            for(int i = 0; i < pos; i++){
                lt += ((double)bucketCount[i]/total);
            }
            lt += (v - vLeft)*((double)bucketCount[pos]/total);
        }


        if(op == Predicate.Op.EQUALS){
            return eqfrac;
        }else if(op == Predicate.Op.GREATER_THAN){
            return gt;
        }else if(op == Predicate.Op.GREATER_THAN_OR_EQ){
            return gt + eqfrac;
        }else if(op == Predicate.Op.LESS_THAN){
            return lt;
        }else if(op == Predicate.Op.LESS_THAN_OR_EQ){
            return lt + eqfrac;
        }else{
            return gt + lt;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return new String("bucktNumber:" + bucketNum);
    }
}
