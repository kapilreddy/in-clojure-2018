(ns inc-clojure-2018.core
  (:require [inc-clojure-2018.helper :as h]
            [clojure.core.async :refer [chan go <! >!
                                        <!! >!! alt! go-loop]]))

;; Problem
;; Kafka -> ??? -> ES

;; +1
(comment
  (let [e (h/init-es-bulk-updater
           (fn [success]
             )
           ;; on error
           (fn [error]
             ))]
    ;; Long running doseq
    (doseq [m (h/kafka-messages "elastic_search_updates")]
      (println "\t" m)
      (h/bulk-process-update e m))))

;; +1
(comment
  (let [e (h/init-es-bulk-updater
           (fn [success]
             (case success
               :es-bulk-start (println "\t" "Bulk start")
               :es-bulk-success (println "\t" "Bulk success")))
           ;; on error
           (fn [error]
             (case error
               :es-slow (println "\t" "ES response time is slow")
               :es-timeout (println "\t" "ES response timed out"))))]
    ;; Long running doseq
    (doseq [m (h/kafka-messages "elastic_search_updates")]
      (println "\t" m)
      (h/bulk-process-update e m))))

;; +1
;; We introduce with-stubbed-events that emulates IO
;; Intro / Happy path
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:es :es-bulk-success]]
    (let [e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success))
             ;; on error
             (fn [error]
               (println "\t" error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (println "\t" m)
        (h/bulk-process-update e m)))))


;; Condition #1
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;; !!!
     [:es :es-bulk-success]

     ]
    (let [e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success))
             ;; on error
             (fn [error]))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (println "\t" m)
        (h/bulk-process-update e m)))))

;; +1
;; Condition #1 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;;; !!!
     [:es :es-bulk-success]]
    (let [success-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (reset! success-a success)
               (println "\t" success))
             ;; on error
             (fn [error]
               ))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :es-bulk-start)
          (loop []
            (when-not (= @success-a
                         :es-bulk-success)
              (recur))))
        (println "\t" m)
        (h/bulk-process-update e m)))))

;; +2
;; Condition #2
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;;!!!
     [:es :es-slow]

     [:kafka "kafka-msg-4"]]
    (let [success-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success)
               (reset! success-a success))
             ;; on error
             (fn [error]
               (println "\t" error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :es-bulk-start)
          (loop []
            (when-not (= @success-a
                         :es-bulk-success)
              (recur))))
        (println "\t" m)
        (h/bulk-process-update e m)))))


;; +2
;; Condition #2 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;; !!!
     [:es :es-slow]

     [:kafka "kafka-msg-4"]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success)
               (reset! success-a success))
             ;; on error
             (fn [error]
               (println "\t" error)
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :es-bulk-start)
          (loop []
            (when-not (or (= @success-a
                             :es-bulk-success)
                          @error-a)
              (recur))))
        (println "\t" m)
        (h/bulk-process-update e m)))))

;; +2
;; Condition #3
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;;!!!
     [:es :es-slow]

     [:kafka "kafka-msg-4"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-5*"] ;;!!!
     [:es :es-bulk-success]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success)
               (reset! success-a success))
             ;; on error
             (fn [error]
               (println "\t" error)
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :es-bulk-start)
          (loop []
            (when (and (not= @success-a
                          :es-bulk-success)
                       (not @error-a))
              (recur))))
        (println "\t" m)
        (h/bulk-process-update e m)))))

;; +2
;; Condition #3 / Solution
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;;!!!
     [:es :es-slow]

     [:kafka "kafka-msg-4"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-5*"] ;;!!!
     [:es :es-bulk-success]]
    (let [success-a (atom nil)
          error-a (atom nil)
          e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success)
               (reset! success-a success))
             ;; on error
             (fn [error]
               (println "\t" error)
               (reset! error-a error)))]
      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (when (= @success-a
                 :es-bulk-start)
          (loop []
            (when (and (not= @success-a
                             :es-bulk-success)
                       (not @error-a))
              (recur))))
        (when @error-a
          (reset! error-a nil)
          (reset! success-a nil))
        (println "\t" m)
        (h/bulk-process-update e m)))))

;; +2
;; Intro to core.async
(comment
  ;; core async intro
  (let [c (chan)]
    (go (>! c "message"))
    (go (println "\t" "Message from channel c "(<! c))))

  (let [c-1 (chan)]
    (go (>! c-1 "message-1")
        (println "\t" "First put succeded"))
    (go (>! c-1 "message-2")
        (println "\t" "Second put succeded"))
    (go (<! c-1)))

  (let [c-2 (chan)]
    (go (>! c-2 "message-1"))
    (go (println "\t" "Message from channel " (<! c-2)))
    (go (println "\t" "Message from channel " (<! c-2))))

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
                  (println "\t" "Message from channel 1 "))
          chan-2 ([msg]
                  (println "\t" "Message from channel 2 ")))))))

;; +2
;; Place core.async
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]
     [:es :es-bulk-start]
     [:es :es-bulk-success]]
    (let [msg-chan (chan)
          e (h/init-es-bulk-updater
             (fn [success]
               (println "\t" success))
             ;; on error
             (fn [error]
               (case error
                 )))]
      (future
        (loop []
          (let [m (<!! msg-chan)]
            (println "\t" m)
            (h/bulk-process-update e m))
          (recur)))

      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (>!! msg-chan m)))))

;; +3
(comment
  (h/with-stubbed-events
    [[:kafka "kafka-msg-1"]
     [:kafka "kafka-msg-2"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-3*"] ;;!!!
     [:es :es-slow]

     [:kafka "kafka-msg-4"]

     [:es :es-bulk-start]
     [:kafka "kafka-msg-5*"] ;;!!!
     [:es :es-bulk-success]]
    (let [control-chan (chan)
          msg-chan (chan)
          e (h/init-es-bulk-updater
             (fn [success]
               (>!! control-chan success)
               (println "\t" success))
             ;; on error
             (fn [error]
               (>!! control-chan error)
               (println "\t" error)))]
      (go-loop [state :init]
        (case state
          :es-bulk-start (recur (<! control-chan))
          (alt!
            control-chan ([msg]
                          (case msg
                            :es-bulk-start (recur :es-bulk-start)
                            :es-bulk-success (do ;; (commit-kafka-offsets)
                                               (recur :es-bulk-success))
                            :es-slow (do (Thread/sleep 5000)
                                         (recur :es-slow))))
            msg-chan ([msg]
                      (println "\t" msg)
                      (h/bulk-process-update e msg)
                      (recur state)))))

      ;; Long running doseq
      (doseq [m (h/kafka-messages "elastic_search_updates")]
        (>!! msg-chan m)))))
