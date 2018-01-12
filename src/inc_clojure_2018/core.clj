(ns in-clojure-2018.core
  (:require [in-clojure-2018.helper :as h]
            [clojure.core.async :refer [chan go <! >! <!! >!! alt! go-loop]]))

;; Problem
;; Kafka -> ??? -> ES
;;
;; h/kafka-messages returns a lazy sequence of Kafka messages as they
;; are published
;;
;; h/init-es-bulk-updater updates ElasticSearch.
;; There  are two callback functions.
;;
;; First one runs for happy path. When Elasticsearch client decides to
;; perform a bulk update and when bulk update succeedes
;;
;; Second callback runs when there is failure response from Elasticsearch
;;
;; The service should not consume messages when bulk update is in progress
;; This is important because we don't know what will be the response and
;; We might consume more messages than we should and in error cases loose
;; this data

(comment
  (let [e (h/init-es-bulk-updater
           (fn [success]
             (case success
               :bulk-start (println "Bulk start")
               :bulk-success (println "Bulk success")))
           ;; on error
           (fn [error]
             (case error
               :es-slow (println "ES response time is slow")
               :es-timeout (println "ES response timed out")
               :es-down (println "ES is down")
               :es-partial-error (println "Partial update errors")
               :es-unknown-error (println "Something went wrong"))))]
    ;; Long running doseq
    (doseq [m (h/kafka-messages "elastic_search_updates")]
      (println m)
      (h/bulk-process-update e m))))

;; We introduce with-stubbed-events that emulates IO
;; Intro / Happy path
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:es :bulk-success]]
    (let [e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success")))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (println m)
        (h/bulk-process-update e m)))))


;; Condition #1
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:kafka "update-9"]
     [:es :bulk-success]]
    (let [e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success")))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (println m)
        (h/bulk-process-update e m)))))

;; Condition #1 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:kafka "update-9"]
     [:es :bulk-success]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (reset! success-a success)
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success")))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :bulk-start)
          (loop []
            (when-not (= @success-a
                         :bulk-success)
              (recur))))
        (println m)
        (h/bulk-process-update e m)))))


;; Condition #2
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:es :es-slow]
     [:kafka "update-9"]
     [:kafka "update-10"]
     [:kafka "update-11"]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success"))
               (reset! success-a success))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :bulk-start)
          (loop []
            (when-not (= @success-a
                         :bulk-success)
              (recur))))
        (println "Receieved kafka message " m)
        (h/bulk-process-update e m)))))

;; Condition #2 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:es :es-slow]
     [:kafka "update-9"]
     [:kafka "update-10"]
     [:kafka "update-11"]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success"))
               (reset! success-a success))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :bulk-start)
          (loop []
            (when (and (= @success-a
                          :bulk-start)
                       (not @error-a))
              (recur))))
        (println "Receieved kafka message " m)
        (h/bulk-process-update e m)))))

;; Condition #3
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:es :es-slow]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:kafka "update-9"]
     [:es :bulk-start]
     [:kafka "update-10"]
     [:kafka "update-11"]
     [:kafka "update-12"]
     [:es :bulk-success]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success"))
               (reset! success-a success))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :bulk-start)
          (loop []
            (when (and (= @success-a
                          :bulk-start)
                       (not @error-a))
              (recur))))
        (println "Receieved kafka message " m)
        (h/bulk-process-update e m)))))

;; Condition #3 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:es :es-slow]
     [:kafka "update-7"]
     [:kafka "update-8"]
     [:kafka "update-9"]
     [:es :bulk-start]
     [:kafka "update-10"]
     [:kafka "update-11"]
     [:kafka "update-12"]
     [:es :bulk-success]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success"))
               (reset! success-a success))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :bulk-start)
          (loop []
            (when (and (= @success-a
                          :bulk-start)
                       (not @error-a))
              (recur))))
        (when @error-a
          (reset! error-a nil)
          (reset! success-a nil))
        (println "Receieved kafka message " m)
        (h/bulk-process-update e m)))))


;; Intro to core.async
(comment
  ;; core async intro
  (let [c (chan)]
    (go (>! c "message"))
    (go (println "Message from channel c "(<! c))))

  (let [c-1 (chan)]
    (go (>! c-1 "message-1")
        (println "First put succeded"))
    (go (>! c-1 "message-1")
        (println "Second put succeded"))
    (go (<! c-1)))

  (let [c-2 (chan)]
    (go (>! c-2 "message-1"))
    (go (println "Message from channel " (<! c-2)))
    (go (println "Message from channel " (<! c-2))))

  ;; alt! example
  (do
    (let [chan-1 (chan)
          chan-2 (chan)]
      (go (Thread/sleep (rand-int 1000))
          (>! chan-1 "message 1"))

      (go (Thread/sleep (rand-int 1000))
          (>! chan-2 "message 1"))

      (go
        (alt!
          chan-1 ([msg]
                  (println "Message from channel 1 "))
          chan-2 ([msg]
                  (println "Message from channel 2 ")))))))


;; Place core.async
(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:es :bulk-success]]
    (let [e (h/init-es-bulk-updater
             (fn [success]
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success")))
             ;; on error
             (fn [error]
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))))
          msg-chan (chan)]
      (future
        (loop []
          (let [m (<!! msg-chan)]
            (println "Receieved kafka message " m)
            (h/bulk-process-update e m))
          (recur)))

      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (>!! msg-chan m)))))


(comment
  (h/with-stubbed-events
    [[:kafka "update-1"]
     [:kafka "update-2"]
     [:kafka "update-3"]
     [:kafka "update-4"]
     [:kafka "update-5"]
     [:es :bulk-start]
     [:kafka "update-6"]
     [:es :bulk-success]
     [:kafka "update-7"]]
    (let [control-chan (chan)
          e (h/init-es-bulk-updater
             (fn [success]
               (>!! control-chan success)
               (case success
                 :bulk-start (println "Bulk start")
                 :bulk-success (println "Bulk success")))
             ;; on error
             (fn [error]
               (>!! control-chan error)
               (case error
                 :es-slow (println "ES response time is slow")
                 :es-timeout (println "ES response timed out")
                 :es-down (println "ES is down")
                 :es-partial-error (println "Partial update errors")
                 :es-unknown-error (println "Something went wrong"))))
          msg-chan (chan)]
      (go-loop [state :init]
        (case state
          :bulk-start (recur (<! control-chan))
          (alt!
            control-chan ([msg]
                          (case msg
                            :bulk-start (recur :bulk-start)
                            :bulk-success (do ;; (commit-kafka-offsets)
                                            (recur :bulk-success))
                            :bulk-partial (recur :consume)
                            :es-down (System/exit 1)
                            :es-slow (do (Thread/sleep 5000)
                                         (recur :es-slow))))
            msg-chan ([msg]
                      (println msg)
                      (h/bulk-process-update e msg)
                      (recur state)))))

      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (>!! msg-chan m)))))
