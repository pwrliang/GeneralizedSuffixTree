package GST.ahocorasick.trie;


import GST.ahocorasick.interval.Interval;
import GST.ahocorasick.interval.Intervalable;

public class Emit extends Interval implements Intervalable {

    private final String keyword;

    public Emit(final int start, final int end, final String keyword) {
        super(start, end);
        this.keyword = keyword;
    }

    public String getKeyword() {
        return this.keyword;
    }

    @Override
    public String toString() {
        return super.toString() + "=" + this.keyword;
    }
}
