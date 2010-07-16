package org.lilycms.util;

public class Pair<T1, T2> {
    private T1 v1;
    private T2 v2;

    public Pair(T1 v1, T2 v2) {
        this.v1 = v1;
        this.v2 = v2;
    }

    public T1 getV1() {
        return v1;
    }

    public T2 getV2() {
        return v2;
    }

    @Override
    public int hashCode() {
        int result = 17;
        if (v1 != null)
            result = 37 * result + v1.hashCode();
        if (v2 != null)
            result = 37 * result + v2.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Pair) {
            Pair other = (Pair)obj;
            return ObjectUtils.safeEquals(v1, other.v1) && ObjectUtils.safeEquals(v2, other.v2);
        }
        return false;
    }
}
