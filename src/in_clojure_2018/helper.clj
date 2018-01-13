(ns in-clojure-2018.helper
  (:require [clojure.core.async :refer [go >! <! chan >!! <!! close! offer!]]))

(defn init-es-bulk-updater
  [success-callback failure-callback]
  [success-callback failure-callback])

(defn bulk-process-updater
  [_ m]
  )

(defn kafka-messages
  [_]
  )

(defn bulk-process-update
  [_ _]
  )

;; https://gist.github.com/stathissideris/8659706
(defn seq!!
  "Returns a (blocking!) lazy sequence read from a channel."
  [c]
  (lazy-seq
   (when-let [v (<!! c)]
     (cons v (seq!! c)))))

(defn with-stubbed-events*
  [events body-fn]
  (println "========================================")
  (println "Input signals")
  (println "========================================")
  (doseq [event events]
    (when-let [m (second event)]
      (println "\t" m)))
  (println "========================================")
  (println "System  behaviour")
  (println "========================================")
  (let [es-chan (chan)
        kafka-chan (chan 10000)]
    (with-redefs [init-es-bulk-updater (fn [success failure]
                                         (future (loop []
                                                   (let [m (<!! es-chan)]
                                                     (if (#{:es-bulk-start :es-bulk-success} m)
                                                       (success m)
                                                       (failure m))
                                                     (when m
                                                       (recur))))))]
      (with-redefs [kafka-messages (constantly (seq!! kafka-chan))]
        (future (let [acc-xs (atom '())]
                  (doseq [[[t m] i] (map vector events (range))]
                    (Thread/sleep (* i 10))
                    (case t
                      :es (>!! es-chan m)
                      :kafka (>!! kafka-chan m)
                      :end (do (doseq [m @acc-xs]
                                 (offer! kafka-chan m))
                               (close! es-chan)
                               (close! kafka-chan))))))
        (body-fn))))
  (println "========================================\n\n"))


(defmacro with-stubbed-events
  [events & body]
  `(with-stubbed-events* (conj ~events [:end])
     (fn []
       ~@body)))
