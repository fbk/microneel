package eu.fbk.microneel.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * Encodes the rewriting of an original string into a rewritten string, allowing mapping offsets in
 * the rewritten string to corresponding offsets in the original string.
 * <p>
 * A {@code Rewriting} object is instantiated with the original string, which is then manipulated
 * via {@link #replace(String, String)} and {@link #replace(int, int, String)}. The
 * {@code Rewriting} objects tracks internally the performed rewrite operations, in order to allow
 * mapping offsets in the rewritten string (obtainable via {@link #getRewrittenString()}) to offsets
 * in the original string (see method {@link #toOriginalOffset(int)}).
 * </p>
 * <p>
 * This class is not thread safe.
 * </p>
 */
public final class Rewriting implements Serializable, Cloneable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Rewriting.class);

    private static final long serialVersionUID = 1L;

    private final String originalString;

    private String rewrittenString;

    private int[] originalOffsets;

    private int[] rewrittenOffsets;

    private boolean[] unchanged;

    /**
     * Creates a new {@code Replacement} based on the data contained in the supplied JSON object.
     *
     * @param json
     *            the JSON object, not null
     */
    public Rewriting(final JsonObject json) {
        this.originalString = json.get("from").getAsString();
        this.rewrittenString = json.get("to").getAsString();
        if (!json.has("replacements")) {
            this.originalOffsets = new int[] { 0, this.originalString.length() };
            this.rewrittenOffsets = new int[] { 0, this.rewrittenString.length() };
            this.unchanged = new boolean[] { true, true };
        } else {
            final Map<Integer, JsonObject> map = new TreeMap<>();
            for (final JsonElement element : (JsonArray) json.get("replacements")) {
                final JsonObject r = (JsonObject) element;
                map.put(r.get("fromOffset").getAsInt(), r);
            }
            final List<Integer> originalOffsets = new ArrayList<>();
            final List<Integer> rewrittenOffsets = new ArrayList<>();
            final List<Boolean> unchanged = new ArrayList<>();
            int originalOffset = 0;
            int rewrittenOffset = 0;
            for (final JsonObject replacement : map.values()) {
                final String from = replacement.get("from").getAsString();
                final String to = replacement.get("to").getAsString();
                final int fromOffset = replacement.get("fromOffset").getAsInt();
                final int toOffset = replacement.get("toOffset").getAsInt();
                if (fromOffset > originalOffset) {
                    originalOffsets.add(originalOffset);
                    rewrittenOffsets.add(rewrittenOffset);
                    unchanged.add(true);
                }
                originalOffsets.add(fromOffset);
                rewrittenOffsets.add(toOffset);
                unchanged.add(false);
                originalOffset = fromOffset + from.length();
                rewrittenOffset = toOffset + to.length();
            }
            originalOffsets.add(originalOffset);
            rewrittenOffsets.add(rewrittenOffset);
            unchanged.add(true);
            if (originalOffset < this.originalString.length()) {
                originalOffsets.add(this.originalString.length());
                rewrittenOffsets.add(this.rewrittenString.length());
                unchanged.add(true);
            }
            this.originalOffsets = Ints.toArray(originalOffsets);
            this.rewrittenOffsets = Ints.toArray(rewrittenOffsets);
            this.unchanged = Booleans.toArray(unchanged);
        }
    }

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
                            && Character.isLetterOrDigit(this.rewrittenString.charAt(start - 1))) {
                        --start;
                    }
                    int end = rewrittenOffset;
                    while (end < this.rewrittenOffsets[i + 1]
                            && Character.isLetterOrDigit(this.rewrittenString.charAt(end))) {
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
                    if (rewrittenOffset == start) {
                        return this.originalOffsets[i] + (Character.isLetterOrDigit(ch) ? 0 : 1);
                    } else if (rewrittenOffset == end) {
                        return this.originalOffsets[i + 1];
                    } else {
                        final char c = this.rewrittenString.charAt(rewrittenOffset);
                        int index = this.originalString.indexOf(c, this.originalOffsets[i]);
                        if (index < 0 || index >= this.originalOffsets[i + 1]) {
                            index = this.originalOffsets[i]
                                    + (Character.isLetterOrDigit(ch) ? 0 : 1);
                        }
                        LOGGER.warn("Mapping ambiguous rewritten offset {} to {} for rewriting {}",
                                rewrittenOffset, index, this);
                        return index;
                    }
                }
            }
        }
        return rewrittenOffset; // must be negative
    }

    /**
     * Attempts to replace the substring of the original string, given by the supplied start and end
     * indexes, with the replacement string specified. Fails if part of the string to replace was
     * already replaced with something else.
     *
     * @param start
     *            the start offset of the substring to replace in the original string
     * @param end
     *            the end offset of the substring to replace in the original string
     * @param replacementString
     *            the replacement string, not null
     * @throws IllegalStateException
     *             if the replacement cannot be performed as part of the original string to replace
     *             was already replaced with something else
     */
    public void replace(final int start, final int end, final String replacementString) {
        if (!tryReplace(start, end, replacementString)) {
            throw new IllegalStateException("Range " + start + ", " + end + " already replaced");
        }
    }

    /**
     * Attempts to replace the substring of the original string, given by the supplied start and end
     * indexes, with the replacement string specified. Do nothing and returns false in case part of
     * the string to replace was already replaced with something else.
     *
     * @param start
     *            the start offset of the substring to replace in the original string
     * @param end
     *            the end offset of the substring to replace in the original string
     * @param replacementString
     *            the replacement string, not null
     * @return true if the replacement was successful, false the replacement cannot be performed as
     *         part of the string to replace was already replaced with something else
     */
    public boolean tryReplace(final int start, final int end, final String replacementString) {
        int index = -1;
        for (int i = 0; i < this.originalOffsets.length - 1; ++i) {
            if (this.originalOffsets[i] <= start && this.originalOffsets[i + 1] >= end) {
                index = i;
                break;
            }
        }

        if (index < 0 || !this.unchanged[index]) {
            return false;
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
        return true;
    }

    /**
     * Replaces all the occurrences of a replaced substring in the original string with the
     * replacement string specified. Fails in case an occurrence of the string to replace cannot be
     * replaced as previously replaced with something else.
     *
     * @param replacedString
     *            the substring to replace, not null
     * @param replacementString
     *            the replacement string, not null
     * @param ignoreCase
     *            if letter case should be ignored when matching the substrings to replace
     * @return the number of replacements performed
     */
    public int replace(String replacedString, final String replacementString,
            final boolean ignoreCase) {
        int result = 0;
        int start = 0;
        String originalString = this.originalString;
        originalString = ignoreCase ? originalString.toLowerCase() : originalString;
        replacedString = ignoreCase ? replacedString.toLowerCase() : replacedString;
        while (true) {
            start = originalString.indexOf(replacedString, start);
            if (start < 0) {
                return result;
            }
            replace(start, start + replacedString.length(), replacementString);
            start += replacedString.length();
            ++result;
        }
    }

    /**
     * Replaces all the occurrences of a replaced substring in the original string with the
     * replacement string specified. Ignores occurrences of the string to replace that cannot be
     * replaced since previously replaced with something else.
     *
     * @param replacedString
     *            the substring to replace, not null
     * @param replacementString
     *            the replacement string, not null
     * @param ignoreCase
     *            if letter case should be ignored when matching the substrings to replace
     * @return the number of replacements performed
     */
    public int tryReplace(String replacedString, final String replacementString,
            final boolean ignoreCase) {
        int result = 0;
        int start = 0;
        String originalString = this.originalString;
        originalString = ignoreCase ? originalString.toLowerCase() : originalString;
        replacedString = ignoreCase ? replacedString.toLowerCase() : replacedString;
        while (true) {
            start = originalString.indexOf(replacedString, start);
            if (start < 0) {
                return result;
            }
            if (tryReplace(start, start + replacedString.length(), replacementString)) {
                ++result;
            }
            start += replacedString.length();
        }
    }

    /**
     * {@inheritDoc} The method returns a deep clone of this object.
     */
    @Override
    public Rewriting clone() {
        try {
            final Rewriting clone = (Rewriting) super.clone();
            clone.originalOffsets = clone.originalOffsets.clone();
            clone.rewrittenOffsets = clone.rewrittenOffsets.clone();
            clone.unchanged = clone.unchanged.clone();
            return clone;
        } catch (final CloneNotSupportedException ex) {
            throw new Error(ex);
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
     * Returns a complete JSON representation of this object.
     *
     * @return a JSON representation
     */
    public JsonObject toJson() {
        final JsonObject json = new JsonObject();
        json.addProperty("from", this.originalString);
        json.addProperty("to", this.rewrittenString);
        final JsonArray replacements = new JsonArray();
        for (int i = 0; i < this.originalOffsets.length - 1; ++i) {
            if (!this.unchanged[i]) {
                final int os = this.originalOffsets[i];
                final int oe = this.originalOffsets[i + 1];
                final int rs = this.rewrittenOffsets[i];
                final int re = this.rewrittenOffsets[i + 1];
                final JsonObject replacement = new JsonObject();
                replacement.addProperty("from", this.originalString.substring(os, oe));
                replacement.addProperty("fromOffset", os);
                replacement.addProperty("to", this.rewrittenString.substring(rs, re));
                replacement.addProperty("toOffset", rs);
                replacements.add(replacement);
            }
        }
        if (replacements.size() > 0) {
            json.add("replacements", replacements);
        }
        return json;
    }

    /**
     * {@inheritDoc} The returned string contains the original and rewritten string, as well as info
     * about the rewriting status of each substring of the original string.
     */
    @Override
    public String toString() {
        return toJson().toString();
    }

}
