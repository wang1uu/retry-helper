package cc.wang1;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * 通用重试工具
 * @author wang1
 */
public class RetryHelper<T> {

    /**
     * 重试条件
     */
    private final Predicate<T> retryOnCondition;

    /**
     * 异常重试条件
     */
    private final Set<Class<? extends Throwable>> retryOnException;

    /**
     * 异常重试条件
     */
    private final HashMap<Class<? extends Throwable>, Predicate<Throwable>> retryWithException;

    /**
     * 重试回调
     */
    private final BiConsumer<T, Throwable> retryListeners;

    /**
     * 退避策略
     */
    private final BiConsumer<T, Throwable> blockStrategy;

    /**
     * 最大重试次数
     */
    private int maxRetryCallLimit = 0;

    private RetryHelper(Predicate<T> retryOnCondition,
                        Set<Class<? extends Throwable>> retryOnException,
                        HashMap<Class<? extends Throwable>, Predicate<Throwable>> retryWithException,
                        BiConsumer<T, Throwable> retryListeners,
                        BiConsumer<T, Throwable> blockStrategy,
                        int maxRetryCallLimit) {

        this.retryOnCondition = retryOnCondition;
        this.retryOnException = retryOnException;
        this.retryWithException = retryWithException;
        this.retryListeners = retryListeners;
        this.blockStrategy = blockStrategy;
        this.maxRetryCallLimit = maxRetryCallLimit;
    }

    @SuppressWarnings("all")
    public T call(Callable<T> task) {
        if (task == null) {
            throw new RuntimeException("The retry task is required.");
        }
        if (blockStrategy == null) {
            throw new RuntimeException("The block strategy is required.");
        }

        T result = null;
        Exception exception = null;

        for (int i=0; i <= maxRetryCallLimit; ++i) {
            try {
                result = task.call();

                if (!retryOnCondition.test(result)) {
                    return result;
                }
            }catch (Exception e) {
                e.printStackTrace();

                exception = e;
                if (!retryOnException.contains(e.getClass())) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                if (retryWithException.get(e.getClass()) != null && !retryWithException.get(e.getClass()).test(e)) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }

            // 重试回调
            try {
                if (retryListeners != null) {
                    retryListeners.accept(result, exception);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }

            // 退避策略
            try {
                blockStrategy.accept(result, exception);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }

        return result;
    }

    public static class RetryHelperBuilder<T> {
        /**
         * 重试条件
         */
        private Predicate<T> retryOnCondition = r -> false;

        /**
         * 异常重试条件
         */
        private final Set<Class<? extends Throwable>> retryOnException = new HashSet<>();

        /**
         * 异常重试条件
         */
        private final HashMap<Class<? extends Throwable>, Predicate<Throwable>> retryWithException = new HashMap<>();

        /**
         * 重试回调
         */
        private BiConsumer<T, Throwable> retryListeners = null;

        /**
         * 退避策略
         */
        private BiConsumer<T, Throwable> blockStrategy = (r, t) -> {};

        /**
         * 最大重试次数
         */
        private int maxRetryCallLimit = 0;

        public static <T> RetryHelperBuilder<T> newBuilder(Class<T> resultType) {
            return new RetryHelperBuilder<>();
        }

        public RetryHelper<T> build() {
            Set<Class<? extends Throwable>> notExist = retryWithException.keySet().stream()
                    .filter(c -> !retryOnException.contains(c))
                    .collect(Collectors.toSet());
            if (!notExist.isEmpty()) {
                throw new RuntimeException(String.format("Retry with exceptions [%s] not exist in retry on exceptions [%s]",
                                           notExist.stream().map(String::valueOf).collect(Collectors.joining(",")),
                                           retryOnException.stream().map(String::valueOf).collect(Collectors.joining(","))));
            }
            return new RetryHelper<>(retryOnCondition, retryOnException, retryWithException, retryListeners, blockStrategy, maxRetryCallLimit);
        }

        public RetryHelperBuilder<T> retryWithListener(BiConsumer<T, Throwable> listener) {
            if (listener == null) {
                return this;
            }
            this.retryListeners = this.retryListeners == null ? listener : this.retryListeners.andThen(listener);
            return this;
        }

        public RetryHelperBuilder<T> retryWithBlockStrategy(BiConsumer<T, Throwable> blockStrategy) {
            if (blockStrategy == null) {
                throw new RuntimeException("The blockStrategy is required.");
            }
            this.blockStrategy = this.blockStrategy.andThen(blockStrategy);
            return this;
        }

        public RetryHelperBuilder<T> retryIfException(Class<? extends Throwable> exception) {
            if (exception == null) {
                return this;
            }
            this.retryOnException.add(exception);
            return this;
        }

        public RetryHelperBuilder<T> retryWithException(Class<? extends Throwable> exceptionClass, Predicate<Throwable> predicate) {
            if (exceptionClass == null || predicate == null) {
                return this;
            }
            this.retryWithException.merge(exceptionClass, predicate, Predicate::or);
            return this;
        }

        public RetryHelperBuilder<T> retryIfCondition(Predicate<T> predicate) {
            if (predicate == null) {
                return this;
            }
            this.retryOnCondition = this.retryOnCondition == null ? predicate : this.retryOnCondition.or(predicate);
            return this;
        }

        public RetryHelperBuilder<T> maxRetryCallLimit(int count) {
            if (count < 0) {
                throw new RuntimeException(String.format("illegal config [%s] on maxRetryLimit(count >= 0).", count));
            }
            this.maxRetryCallLimit = count;
            return this;
        }
    }
}
