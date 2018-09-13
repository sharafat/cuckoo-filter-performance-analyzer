package bd.ac.buet.cse.ms.thesis.legacy.utils;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.SourceDataLine;

public class SoundUtils {

    private static float SAMPLE_RATE = 8000f;

    public static void tone(int hz, int msecs) {
        tone(hz, msecs, 1.0);
    }

    public static void tone(int hz, int msecs, double vol) {
        byte[] buf = new byte[1];
        AudioFormat af =
                new AudioFormat(
                        SAMPLE_RATE, // sampleRate
                        8,           // sampleSizeInBits
                        1,           // channels
                        true,        // signed
                        false);      // bigEndian
        SourceDataLine sdl;
        try {
            sdl = AudioSystem.getSourceDataLine(af);
            sdl.open(af);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        sdl.start();
        for (int i = 0; i < msecs * 8; i++) {
            double angle = i / (SAMPLE_RATE / hz) * 2.0 * Math.PI;
            buf[0] = (byte) (Math.sin(angle) * 127.0 * vol);
            sdl.write(buf, 0, 1);
        }
        sdl.drain();
        sdl.stop();
        sdl.close();
    }
}
