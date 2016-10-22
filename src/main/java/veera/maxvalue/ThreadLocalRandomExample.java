/**
 * 
 */
package veera.maxvalue;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author hadoop1
 *
 */
public class ThreadLocalRandomExample {

    /**
     * @param args
     */
    public static void main(String[] args) {

        List<String> list = new ArrayList<String>();
        list.add("RAVI");
        list.add("VEERA");
        list.add("KUMAR");
        list.add("SINGIRI");
        list.add("DHATRI");

        /*
         * ThreadLocalRandomExample obj = new ThreadLocalRandomExample();
         * for(int i = 0; i < 10; i++){
         * System.out.println(obj.getRandomList(list)); }
         */

        String items[] = { "RAVI", "VEERA", "KUMAR", "SINGIRI", "DHATRI" };
        String items2[] = { "2", "50", "12", "123", "1" };
        int amount;
        String list2, list3;
        Random r = new Random();

        amount = (int) (Math.random() * 1000 + 1);// used to determine the
                                                  // amount
                                                  // of cycles of getting a
                                                  // random
                                                  // string in items[] array.
        for (int i = amount; i > 0; --i) {
            list2 = items[r.nextInt(5)];// The variable I would like to use to
                                        // store all of the randomly picked
                                        // strings
            list3 = items2[r.nextInt(5)];
            System.out.println(list2 + "," + list3);// this is how I want to
                                                    // output the list
            // variable.

        }
    }

    public int getRandomList(List<Integer> list) {

        // 0-4
        int index = ThreadLocalRandom.current().nextInt(list.size());
        System.out.println("\nIndex :" + index);
        return list.get(index);

    }

}
