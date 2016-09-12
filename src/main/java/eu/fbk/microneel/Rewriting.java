package eu.fbk.microneel;

import java.util.Arrays;
import java.util.Objects;

/**
 * Encodes the rewriting of an original string into a rewritten string, allowing mapping offsets in
 * the rewritten string to corresponding offsets in the original string.
 * <p>
 * A {@code Rewriting} object is instantiated with the original string, which is then manipulated
 * via {@link #rewrite(String, String)} and {@link #rewrite(int, int, String)}. The
 * {@code Rewriting} objects tracks internally the performed rewrite operations, in order to allow
 * mapping offsets in the rewritten string (obtainable via {@link #getRewrittenString()}) to offsets
 * in the original string (see method {@link #toOriginalOffset(int)}).
 * </p>
 * <p>
 * This class is not thread safe.
 * </p>
 */
public final class Rewriting {

    private final String originalString;

    private String rewrittenString;

    private int[] originalOffsets;

    private int[] rewrittenOffsets;

    private boolean[] unchanged;

    /**
     * Creates a new {@code Rewriting} object operating on the specified original string.
     *
     * @param originalString
     *            the original string, not null
     */
    public Rewriting(final String originalString) {
        this.originalString = Objects.requireNonNull(originalString);
        this.rewrittenString = originalString;
        this.originalOffsets = new int[] { 0, originalString.length() };
        this.rewrittenOffsets = new int[] { 0, originalString.length() };
        this.unchanged = new boolean[] { true, true };
    }

    /**
     * Returns the original string.
     *
     * @return the original string
     */
    public String getOriginalString() {
        return this.originalString;
    }

    /**
     * Returns the rewritten string.
     *
     * @return the rewritten string
     */
    public String getRewrittenString() {
        return this.rewrittenString;
    }

    /**
     * Maps the supplied offset referring to the rewritten string to the corresponding offset in the
     * original string.
     *
     * @param rewrittenOffset
     *            the offset to map referring to the rewritten string
     * @return the corresponding offset in the original string
     */
    public int toOriginalOffset(final int rewrittenOffset) {
        for (int i = this.rewrittenOffsets.length - 1; i >= 0; --i) {
            if (this.rewrittenOffsets[i] <= rewrittenOffset) {
                final int delta = rewrittenOffset - this.rewrittenOffsets[i];
                if (this.unchanged[i]) {
                    return this.originalOffsets[i] + delta;
                } else {
                    int start = rewrittenOffset;
                    while (start > this.rewrittenOffsets[i]
                            && !Character.isWhitespace(this.rewrittenString.charAt(start - 1))) {
                        --start;
                    }
                    int end = rewrittenOffset;
                    while (end < this.rewrittenOffsets[i + 1]
                            && !Character.isWhitespace(this.rewrittenString.charAt(end))) {
                        ++end;
                    }
                    final String rewrittenToken = this.rewrittenString.toLowerCase()
                            .substring(start, end);
                    final String[] originalTokens = this.originalString
                            .substring(this.originalOffsets[i], this.originalOffsets[i + 1])
                            .toLowerCase().split("\\s+");
                    for (final String originalToken : originalTokens) {
                        final int index = originalToken.indexOf(rewrittenToken);
                        if (index >= 0) {
                            final int originalStart = this.originalString.toLowerCase()
                                    .indexOf(originalToken);
                            return originalStart + index + rewrittenOffset - start;
                        }
                    }
                    final char ch = this.originalString.charAt(this.originalOffsets[i]);
                    return this.originalOffsets[i] + (Character.isLetterOrDigit(ch) ? 0 : 1);
                }
            }
        }
        return rewrittenOffset; // must be negative
    }

    /**
     * Replaces the substring of the original string, given by the supplied start and end indexes,
     * with the replacement string specified.
     *
     * @param start
     *            the start offset of the substring to replace in the original string
     * @param end
     *            the end offset of the substring to replace in the original string
     * @param replacementString
     *            the replacement string, not null
     */
    public void rewrite(final int start, final int end, final String replacementString) {

        int index = -1;
        for (int i = 0; i < this.originalOffsets.length - 1; ++i) {
            if (this.originalOffsets[i] <= start && this.originalOffsets[i + 1] >= end) {
                index = i;
                break;
            }
        }

        if (index < 0) {
            throw new IllegalArgumentException("Invalid offsets " + start + ", " + end
                    + " (string length: " + this.originalString.length() + ")");
        }

        if (!this.unchanged[index]) {
            throw new IllegalArgumentException("Range " + start + ", " + end + " already replaced");
        }

        final boolean hasBefore = start > this.originalOffsets[index];
        final boolean hasAfter = end < this.originalOffsets[index + 1];
        final int newLength = this.originalOffsets.length + (hasBefore ? 1 : 0)
                + (hasAfter ? 1 : 0);
        final int indexAfter = index + 1 + (hasBefore ? 1 : 0) + (hasAfter ? 1 : 0);
        final int lengthAfter = this.originalOffsets.length - (index + 1);
        final int delta = replacementString.length() - (end - start);

        this.originalOffsets = Arrays.copyOf(this.originalOffsets, newLength);
        this.rewrittenOffsets = Arrays.copyOf(this.rewrittenOffsets, newLength);
        this.unchanged = Arrays.copyOf(this.unchanged, newLength);

        if (indexAfter != index + 1) {
            System.arraycopy(this.unchanged, index + 1, this.unchanged, indexAfter, lengthAfter);
            System.arraycopy(this.originalOffsets, index + 1, this.originalOffsets, indexAfter,
                    lengthAfter);
        }
        if (indexAfter != index + 1 || delta != 0) {
            for (int i = lengthAfter - 1; i >= 0; --i) {
                this.rewrittenOffsets[indexAfter + i] = this.rewrittenOffsets[index + 1 + i]
                        + delta;
            }
        }

        final int i = hasBefore ? index + 1 : index;
        this.originalOffsets[i] = start;
        this.rewrittenOffsets[i] = hasBefore
                ? this.rewrittenOffsets[i - 1] + start - this.originalOffsets[i - 1]
                : this.rewrittenOffsets[i];
        this.unchanged[i] = false;

        if (hasAfter) {
            this.originalOffsets[i + 1] = end;
            this.rewrittenOffsets[i + 1] = this.rewrittenOffsets[i] + replacementString.length();
            this.unchanged[i + 1] = true;
        }

        this.rewrittenString = this.rewrittenString.substring(0, this.rewrittenOffsets[i])
                + replacementString
                + this.rewrittenString.substring(this.rewrittenOffsets[i] + end - start);
    }

    /**
     * Replaces all the occurrences of a replaced substring in the original string with the
     * replacement string specified
     *
     * @param replacedString
     *            the substring to replace, not null
     * @param replacementString
     *            the replacement string, not null
     */
    public void rewrite(final String replacedString, final String replacementString) {
        int start = 0;
        while (true) {
            start = this.originalString.indexOf(replacedString, start);
            if (start < 0) {
                return;
            }
            rewrite(start, start + replacedString.length(), replacementString);
            start += replacedString.length();
        }
    }

    /**
     * {@inheritDoc} Two {@code Rewriting} objects are equal if they encode exactly the same
     * rewriting.
     */
    @Override
    public boolean equals(final Object object) {
        if (object == this) {
            return true;
        }
        if (!(object instanceof Rewriting)) {
            return false;
        }
        final Rewriting other = (Rewriting) object;
        return this.originalString.equals(other.originalString)
                && this.rewrittenString.equals(other.rewrittenString)
                && Arrays.equals(this.originalOffsets, other.originalOffsets)
                && Arrays.equals(this.rewrittenOffsets, other.rewrittenOffsets)
                && Arrays.equals(this.unchanged, other.unchanged);
    }

    /**
     * {@inheritDoc} The returned hash code depends on the original string, the rewritten string,
     * and all the parameters characterizing the rewriting.
     */
    @Override
    public int hashCode() {
        return Objects.hash(this.originalString, this.rewrittenString, this.originalOffsets,
                this.rewrittenOffsets, this.unchanged);
    }

    /**
     * {@inheritDoc} The returned string contains the original and rewritten string, as well as info
     * about the rewriting status of each substring of the original string.
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("original:  '").append(this.originalString).append("'\n");
        builder.append("rewritten: '").append(this.rewrittenString).append("'\n");
        for (int i = 0; i < this.originalOffsets.length - 1; ++i) {
            final int os = this.originalOffsets[i];
            final int oe = this.originalOffsets[i + 1];
            final int rs = this.rewrittenOffsets[i];
            final int re = this.rewrittenOffsets[i + 1];
            final String status = this.unchanged[i] ? "unchanged" : "changed";
            builder.append("  '");
            builder.append(this.originalString.substring(os, oe));
            builder.append("' (");
            builder.append(os).append(", ").append(oe).append(", ").append(status);
            builder.append(") ==> '");
            builder.append(this.rewrittenString.substring(rs, re));
            builder.append("' (");
            builder.append(rs).append(", ").append(re);
            builder.append(")\n");
        }
        return builder.toString();
    }

}
