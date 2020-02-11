/*
 * Copyright 2017 ObjectBox Ltd. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.objectbox.rx;

import java.util.List;

import io.objectbox.query.Query;
import io.objectbox.reactive.DataObserver;
import io.objectbox.reactive.DataSubscription;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
import io.reactivex.FlowableOnSubscribe;
import io.reactivex.Observable;
import io.reactivex.ObservableEmitter;
import io.reactivex.ObservableOnSubscribe;
import io.reactivex.Single;
import io.reactivex.SingleEmitter;
import io.reactivex.SingleOnSubscribe;
import io.reactivex.functions.Cancellable;

/**
 * Static methods to Rx-ify ObjectBox queries.
 */
public abstract class RxQuery {
    /**
     * The returned Flowable emits Query results one by one. Once all results have been processed, onComplete is called.
     * Uses BackpressureStrategy.BUFFER.
     */
    public static <T> Flowable<T> flowableOneByOne(final Query<T> query) {
        return flowableOneByOne(query, BackpressureStrategy.BUFFER);
    }

    /**
     * The returned Flowable emits Query results one by one. Once all results have been processed, onComplete is called.
     * Uses given BackpressureStrategy.
     */
    public static <T> Flowable<T> flowableOneByOne(final Query<T> query, BackpressureStrategy strategy) {
        return Flowable.create(new FlowableOnSubscribe<T>() {
            @Override
            public void subscribe(final FlowableEmitter<T> emitter) throws Exception {
                createListItemEmitter(query, emitter);
            }

        }, strategy);
    }

    static <T> void createListItemEmitter(final Query<T> query, final FlowableEmitter<T> emitter) {
        final FlowableEmitter<T> serializedEmitter = emitter.serialize();
        final DataSubscription dataSubscription = query.subscribe().observer(new DataObserver<List<T>>() {
            @Override
            public void onData(List<T> data) {
                for (T datum : data) {
                    if (serializedEmitter.isCancelled()) {
                        return;
                    } else {
                        serializedEmitter.onNext(datum);
                    }
                }
                if (!serializedEmitter.isCancelled()) {
                    serializedEmitter.onComplete();
                }
            }
        });
        serializedEmitter.setCancellable(new Cancellable() {
            @Override
            public void cancel() throws Exception {
                dataSubscription.cancel();
            }
        });
    }

    /**
     * The returned Observable emits Query results as Lists.
     * Never completes, so you will get updates when underlying data changes
     * (see {@link Query#subscribe()} for details).
     */
    public static <T> Observable<List<T>> observable(final Query<T> query) {
        return Observable.create(new ObservableOnSubscribe<List<T>>() {
            @Override
            public void subscribe(final ObservableEmitter<List<T>> emitter) throws Exception {
                final ObservableEmitter<List<T>> serializedEmitter = emitter.serialize();
                final DataSubscription dataSubscription = query.subscribe().observer(new DataObserver<List<T>>() {
                    @Override
                    public void onData(List<T> data) {
                        if (!serializedEmitter.isDisposed()) {
                            serializedEmitter.onNext(data);
                        }
                    }
                });
                serializedEmitter.setCancellable(new Cancellable() {
                    @Override
                    public void cancel() throws Exception {
                        dataSubscription.cancel();
                    }
                });
            }
        });
    }

    /**
     * The returned Single emits one Query result as a List.
     */
    public static <T> Single<List<T>> single(final Query<T> query) {
        return Single.create(new SingleOnSubscribe<List<T>>() {
            @Override
            public void subscribe(final SingleEmitter<List<T>> emitter) throws Exception {
                query.subscribe().single().observer(new DataObserver<List<T>>() {
                    @Override
                    public void onData(List<T> data) {
                        if (!emitter.isDisposed()) {
                            emitter.onSuccess(data);
                        }
                    }
                });
                // no need to cancel, single never subscribes
            }
        });
    }
}
