import java.math.BigDecimal;

/**
 * falcon -- 2016/11/28.
 */
public class Test {
    public static void main(String[] args) {
        BigDecimal bigDecimal = new BigDecimal(2) ;
        bigDecimal = bigDecimal.divide(new BigDecimal(3),3,BigDecimal.ROUND_HALF_UP) ;
        System.out.println(bigDecimal.doubleValue());


        System.out.println(1/3);
    }
}
