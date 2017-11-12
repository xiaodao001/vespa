package com.yahoo.vespa.hosted.controller;

import com.yahoo.transaction.Mutex;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public abstract class Locks<Thing, Id, Lock extends Mutex> {

    public abstract Lock lock(Id id);

    public abstract Optional<Thing> get(Id id);

    public abstract void store(Thing thing);


    public abstract class Lockable {

        private final Change change;

        protected Lockable(Change change) {
            this.change = change;
        }

        public abstract Id id();

        /** Get an updated thing from the repository and merge the changes from this into that. */
        public Optional<Thing> pull() {
            return get(id()).map(change.change(false));
        }

        /** Store the changes of this to the repository, marking its change, and those of its parents, as stored. */
        public void push() {
            try (Lock lock = lock()) {
                get(id()).map(change.change(true)).ifPresent(Locks.this::store);
            }
        }

        public Lock lock() {
            return Locks.this.lock(id());
        }

        public abstract Thing withChange(Function<Thing, Thing> change);


        public final class Change {

            private final Function<Thing, Thing> change;
            private Change before;
            private AtomicBoolean stored;

            protected Change(Function<Thing, Thing> change) {
                this.change = Objects.requireNonNull(change);
                this.stored = new AtomicBoolean(false);
            }

            public Change after(Change before) {
                this.before = before;
                return this;
            }

            /** Gather the changes from the parents of this, and this, and prevent these from being gathered again if store is true. */
            public Function<Thing, Thing> change(boolean store) {
                if ((store && stored.getAndSet(true)) || stored.get()) return Function.identity();
                Function<Thing, Thing> changes = before.change(store).andThen(change);
                if (store) before = null;  // This is only to allow GC to take place, in case of long-lived Things.
                return changes;
            }

        }

    }

}
