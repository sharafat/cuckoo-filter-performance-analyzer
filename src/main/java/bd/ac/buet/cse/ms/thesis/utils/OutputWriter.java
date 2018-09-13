package bd.ac.buet.cse.ms.thesis.utils;

public abstract class OutputWriter {

    public abstract void write(Object obj);

    public void writeLine(Object obj) {
        write(obj + "\n");
    }
}
