package shortPath;

import java.util.*; 


public final class FibonacciHeap<T> {

    public static final class Entry<T> {
        private int     mDegree = 0;       
        private boolean mIsMarked = false; 

        private Entry<T> mNext;  
        private Entry<T> mPrev;

        private Entry<T> mParent; 
        private Entry<T> mChild;  

        private T      mElem;     
        private double mPriority;


        public T getValue() {
            return mElem;
        }

        public void setValue(T value) {
            mElem = value;
        }

        public double getPriority() {
            return mPriority;
        }

        private Entry(T elem, double priority) {
            mNext = mPrev = this;
            mElem = elem;
            mPriority = priority;
        }
    }

    private Entry<T> mMin = null;

    private int mSize = 0;

    public Entry<T> enqueue(T value, double priority) {
        checkPriority(priority);

        Entry<T> result = new Entry<T>(value, priority);

        mMin = mergeLists(mMin, result);

        ++mSize;

        return result;
    }

    public Entry<T> min() {
        if (isEmpty())
            throw new NoSuchElementException("Heap is empty.");
        return mMin;
    }

    public boolean isEmpty() {
        return mMin == null;
    }

    public int size() {
        return mSize;
    }

    public static <T> FibonacciHeap<T> merge(FibonacciHeap<T> one, FibonacciHeap<T> two) {
        FibonacciHeap<T> result = new FibonacciHeap<T>();

        result.mMin = mergeLists(one.mMin, two.mMin);

        result.mSize = one.mSize + two.mSize;

        one.mSize = two.mSize = 0;
        one.mMin  = null;
        two.mMin  = null;

        return result;
    }

    public Entry<T> dequeueMin() {
        if (isEmpty())
            throw new NoSuchElementException("Heap is empty.");

        --mSize;

        Entry<T> minElem = mMin;

        if (mMin.mNext == mMin) { 
            mMin = null;
        }
        else { 
            mMin.mPrev.mNext = mMin.mNext;
            mMin.mNext.mPrev = mMin.mPrev;
            mMin = mMin.mNext; 
        }

        if (minElem.mChild != null) {
            Entry<?> curr = minElem.mChild;
            do {
                curr.mParent = null;


                curr = curr.mNext;
            } while (curr != minElem.mChild);
        }

        mMin = mergeLists(mMin, minElem.mChild);

        if (mMin == null) return minElem;

        List<Entry<T>> treeTable = new ArrayList<Entry<T>>();

        List<Entry<T>> toVisit = new ArrayList<Entry<T>>();

        for (Entry<T> curr = mMin; toVisit.isEmpty() || toVisit.get(0) != curr; curr = curr.mNext)
            toVisit.add(curr);

        for (Entry<T> curr: toVisit) {
            while (true) {
                while (curr.mDegree >= treeTable.size())
                    treeTable.add(null);
                if (treeTable.get(curr.mDegree) == null) {
                    treeTable.set(curr.mDegree, curr);
                    break;
                }

                Entry<T> other = treeTable.get(curr.mDegree);
                treeTable.set(curr.mDegree, null); // Clear the slot

                Entry<T> min = (other.mPriority < curr.mPriority)? other : curr;
                Entry<T> max = (other.mPriority < curr.mPriority)? curr  : other;

                max.mNext.mPrev = max.mPrev;
                max.mPrev.mNext = max.mNext;
                max.mNext = max.mPrev = max;
                min.mChild = mergeLists(min.mChild, max);
                                max.mParent = min;
                max.mIsMarked = false;
                ++min.mDegree;
                curr = min;
            }


            if (curr.mPriority <= mMin.mPriority) mMin = curr;
        }
        return minElem;
    }


    public void decreaseKey(Entry<T> entry, double newPriority) {
        checkPriority(newPriority);
        if (newPriority > entry.mPriority)
            throw new IllegalArgumentException("New priority exceeds old.");

        decreaseKeyUnchecked(entry, newPriority);
    }
    

    public void delete(Entry<T> entry) {

        decreaseKeyUnchecked(entry, Double.NEGATIVE_INFINITY);

        dequeueMin();
    }

    private void checkPriority(double priority) {
        if (Double.isNaN(priority))
            throw new IllegalArgumentException(priority + " is invalid.");
    }

    private static <T> Entry<T> mergeLists(Entry<T> one, Entry<T> two) {

        if (one == null && two == null) { 
            return null;
        }
        else if (one != null && two == null) { 
            return one;
        }
        else if (one == null && two != null) {
            return two;
        }
        else { 
            Entry<T> oneNext = one.mNext; 
            one.mNext = two.mNext;
            one.mNext.mPrev = one;
            two.mNext = oneNext;
            two.mNext.mPrev = two;

            return one.mPriority < two.mPriority? one : two;
        }
    }


    private void decreaseKeyUnchecked(Entry<T> entry, double priority) {
        entry.mPriority = priority;


        if (entry.mParent != null && entry.mPriority <= entry.mParent.mPriority)
            cutNode(entry);


        if (entry.mPriority <= mMin.mPriority)
            mMin = entry;
    }


    private void cutNode(Entry<T> entry) {
        entry.mIsMarked = false;

        if (entry.mParent == null) return;

        if (entry.mNext != entry) { // Has siblings
            entry.mNext.mPrev = entry.mPrev;
            entry.mPrev.mNext = entry.mNext;
        }


        if (entry.mParent.mChild == entry) {
            if (entry.mNext != entry) {
                entry.mParent.mChild = entry.mNext;
            }

            else {
                entry.mParent.mChild = null;
            }
        }

        --entry.mParent.mDegree;

        entry.mPrev = entry.mNext = entry;
        mMin = mergeLists(mMin, entry);

        if (entry.mParent.mIsMarked)
            cutNode(entry.mParent);
        else
            entry.mParent.mIsMarked = true;

        entry.mParent = null;
    }
}