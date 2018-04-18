package org.noip.kugland.kotlin.concurrent

import java.lang.reflect.Method
import java.util.concurrent.Executor

/**
 * Annotation for methods that subscribe to events.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class Subscriber(val executorId: Int = -1)

class ExecutorIdAlreadyRegisteredException : Exception()
class UnknownExecutorException: Exception()

/**
 * Executor that executes runnables in the current thread.
 */
class CallingThreadExecutor: Executor {
    override fun execute(command: Runnable?) {
        command?.run()
    }
}

/**
 * EventBus
 */
class EventBus(val name: String) {

    companion object {
        private val executors: MutableMap<Int, Executor> = mutableMapOf()

        /**
         * Add an executor to EventBus.
         *
         * @param id       id of the executor
         * @param executor the executor
         */
        @Suppress("unused")
        fun addExecutor(id: Int, executor: Executor) {
            executors.putIfAbsent(id, executor)?.let {
                throw ExecutorIdAlreadyRegisteredException()
            }
        }

        /**
         * Remove an executor from EventBus.
         *
         * @param id id of the executor
         */
        @Suppress("unused")
        fun removeExecutor(id: Int) { executors.remove(id) }

        init { addExecutor(-1, CallingThreadExecutor()) }
    }

    /**
     * Maps subscriber classes to (maps of event classes to (lists of (pairs of subscriber methods and executor ids))).
     *
     * i.e.,
     *
     * ```{
     *   subscribing class₁ -> { event class₁₁ -> [ (method₁₁₁, executor id₁₁₂), (method₁₁₂, executor id₁₁₂), ... ],
     *                           event class₁₂ -> [ (method₁₁₁, executor id₁₂₂), (method₁₁₂, executor id₁₂₂), ... ],
     *                           ... },
     *   subscribing class₂ -> { event class₂₁ -> [ (method₂₁₁, executor id₂₁₂), (method₂₁₂, executor id₂₁₂), ... ],
     *                           event class₂₂ -> [ (method₂₁₁, executor id₂₂₂), (method₂₁₂, executor id₂₂₂), ... ],
     *                           ... },
     *   ...
     * }```
     */
    private val methodMapping: MutableMap<Class<*>, Map<Class<*>, List<Pair<Method, Int>>>> = mutableMapOf()

    /**
     * Maps event classes to object instances.
     *
     * i.e.,
     *
     * ```{
     *   event class₁ -> [ subscribing object₁₁, subscribing object₁₂, ... ],
     *   event class₂ -> [ subscribing object₂₁, subscribing object₂₂, ... ],
     *   ...
     * }```
     */
    private val objectMapping: MutableMap<Class<*>, MutableSet<Any>> = mutableMapOf()

    /**
     * Subscribe to this EventBus instance.
     *
     * @param obj subscribing object
     */
    @Suppress("unused")
    fun subscribe(obj: Any) {
        methodMapping.computeIfAbsent(obj.javaClass) {
            obj.javaClass.methods
                    // find all methods that have the Subscriber annotation and have arity 1.
                    .filter { it.annotations.any { it is Subscriber } && it.parameters.size == 1 }
                    // maps to the pair (method, executorId)
                    .map { it to (it.annotations.find { it is Subscriber } as Subscriber).executorId }
                    // group by event class (i.e. the type of the first—and only—parameter)
                    .groupByTo(mutableMapOf()) { it.first.parameters[0].type }
        }.forEach {
            // now, having the mappings { eventClass -> [ ... ] }, add the subscribing object
            // to objectMapping.
            eventClass, _ -> objectMapping.computeIfAbsent(eventClass) { mutableSetOf() }.add(obj)
        }
    }

    /**
     * Unsubscribe from this EventBus instance.
     *
     * @param obj unsubscribing object
     */
    @Suppress("unused")
    fun unsubscribe(obj: Any) {
        objectMapping.forEach { _, objectList -> objectList.remove(obj) }
    }

    /**
     * Send an event to subscribing objects.
     *
     * @param event event to be sent
     */
    @Suppress("unused")
    fun send(event: Any) {
        event.javaClass.let {
            // First get all objects that subscribe to this type of event.
            eventClass -> objectMapping[eventClass]?.forEach {
                // Then get the pairs [(method₁, executor₁), (method₂, executor₂), ...] for the object.
                obj -> methodMapping[obj.javaClass]?.let {
                    // Now, for each of these pairs, ...
                    it[eventClass]?.forEach { (method, executorId) ->
                        executors[executorId].let {
                            // Invoke the method using the corresponding executor.
                            if (it != null) it.execute { method.invoke(obj, event) }
                            // Or throw an exception if we don’t know the executor.
                            else throw UnknownExecutorException()
                        }
                    }
                }
            }
        }
    }

}