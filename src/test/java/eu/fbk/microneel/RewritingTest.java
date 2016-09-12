package eu.fbk.microneel;

import org.junit.Test;

public class RewritingTest {

    @Test
    public void test() {

        final Rewriting r = new Rewriting("@john ke #bellacosa #forzainter");

        System.out.println(r);
        r.rewrite("ke", "che");
        System.out.println(r);
        r.rewrite(0, 5, "John Smith");
        System.out.println(r);
        r.rewrite("#bellacosa", "bella cosa");
        System.out.println(r);
        r.rewrite("#forzainter", "forza Inter");
        System.out.println(r);

        final int s1 = r.toOriginalOffset(r.getRewrittenString().indexOf("John"));
        final int e1 = r.toOriginalOffset(r.getRewrittenString().indexOf("John") + 4);
        System.out.println(r.getOriginalString().substring(s1, e1) + " - " + s1 + ", " + e1);

        final int s2 = r.toOriginalOffset(r.getRewrittenString().indexOf("che"));
        final int e2 = r.toOriginalOffset(r.getRewrittenString().indexOf("che") + 3);
        System.out.println(r.getOriginalString().substring(s2, e2) + " - " + s2 + ", " + e2);

        final int s3 = r.toOriginalOffset(r.getRewrittenString().indexOf("cosa"));
        final int e3 = r.toOriginalOffset(r.getRewrittenString().indexOf("cosa") + 4);
        System.out.println(r.getOriginalString().substring(s3, e3) + " - " + s3 + ", " + e3);

        final int s4 = r.toOriginalOffset(r.getRewrittenString().indexOf("Inter"));
        final int e4 = r.toOriginalOffset(r.getRewrittenString().indexOf("Inter") + 5);
        System.out.println(r.getOriginalString().substring(s4, e4) + " - " + s4 + ", " + e4);

        final int s5 = r.toOriginalOffset(r.getRewrittenString().indexOf("forza Inter"));
        final int e5 = r.toOriginalOffset(r.getRewrittenString().indexOf("forza Inter") + 11);
        System.out.println(r.getOriginalString().substring(s5, e5) + " - " + s5 + ", " + e5);
    }

}
