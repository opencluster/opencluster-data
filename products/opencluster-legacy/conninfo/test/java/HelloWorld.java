import com.oracle.jna.Library;
import com.oracle.jna.Native;

public class HelloWorld {
    public interface CTest extends Library {
        public void helloFromC();
    }
    static public void main(String argv[]) {
        CTest ctest = (CTest) Native.loadLibrary("ctest", CTest.class);
        ctest.helloFromC();
    }
}
