(ns in-clojure-talk.core)

(let [e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [success]
                                   (case msg
                                     ::bulk-success (println "Bulk update succeded")))
                                 ;; on error
                                 (fn [error]
                                   (case error
                                     ::es-slow (println "ES response time is slow")
                                     ::es-down (println "ES is down")
                                     ::es-partial-error (println "Partial update errors")
                                     ::es-unknown-error (println "Something went wrong"))))]
  ;; Long running doseq
  (doseq [m (kafka-messages "elastic_search_updates")]
    (bulk-process-update e m)))


(let [success-a (atom)
      error-a (atom)
      e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [success]
                                   (reset! success-a success))
                                 ;; on error
                                 (fn [error]
                                   (reset! err-a err)))]
  ;; Long running doseq
  (doseq [m (kafka-messages "elastic_search_updates")]
    (case error-a
      ::es-down (do (println "ES is down")
                    (System/exit 1)))
    (bulk-process-update e m)))

(let [success-a (atom)
      error-a (atom)
      e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [success]
                                   (reset! success-a success))
                                 ;; on error
                                 (fn [error]
                                   (reset! err-a err)))]
  ;; Long running doesq
  (doseq [m (kafka-messages "elastic_search_updates")]
    (case error-a
      ::es-down (do (println "ES is down")
                    (System/exit 1))
      ::es-slow (Thread/sleep 1000))
    (case success-a
      ::bulk-success (println "Bulk Success"))
    (bulk-process-update e m)))

;; core async intro
(def c (chan))
(>! c "message")
(<! c)

(go (>! chan-1 "message-1"))
(go (println "Message from channel " (<! chan-1)))

(go (>! chan-1 "message-1"))
(go (println "Message from channel " (<! chan-1)))
(go (println "Message from channel " (<! chan-1)))

;; alt! example
(go (Thread/sleep (rand-int 1000))
    (>! chan-1 "message 1"))

(go (Thread/sleep (rand-int 1000))
    (>! chan-2 "message 1"))

(go
  (alt!
    chan-1 ([msg]
            (println "Message from channel 1 " msg))
    chan-2 ([msg]
            (println "Message from channel 1 " msg))))


;; A long running doseq
(let [e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [msg]
                                   )
                                 ;; on error
                                 (fn [err]
                                   ))]
  (doseq [m (kafka-messages "elastic_search_updates")]
    (>!! msg-chan m))

  ;; Another thread
  (future
    (loop []
      (bulk-process-update (<!! msg-chan))
      (recur))))



(let [e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [msg]
                                   )
                                 ;; on error
                                 (fn [err]
                                   ))]
  (doseq [m (kafka-messages "elastic_search_updates")]
    (>!! msg-chan m))

  ;; Another thread
  (future
    (loop []
      (bulk-process-update (<!! msg-chan))
      (recur))))



(let [e (map->ESAsyncBulkUpdater "localhost"
                                 9001
                                 ;; on success
                                 (fn [msg]
                                   (>!! control-chan ::success))
                                 ;; on error
                                 (fn [err]
                                   (>!! control-chan ::error)))]
  (doseq [m (kafka-messages "elastic_search_updates")]
    (>!! msg-chan m))

  ;; Another thread
  (future
    (loop []
      (alt!
        control-chan (System/exit 1)
        msg-chan ([msg]
                  (bulk-process-update msg)))
      (recur))))



(go-loop []
  (alt!
    (control-chan ([msg]
                   (case msg
                     ::es-down (System/exit 1)
                     ::es-slow (do (Thread/sleep 4000)
                                   (recur)))))
    (msg-chan ([msg]
               (bulk-process-update msg)
               (recur)))))


(go-loop [state :init]
  (alt!
    (control-chan ([msg]
                   (case msg
                     ::bulk-success (do (commit-kafka-offsets)
                                        (recur ::bulk-success))
                     ::bulk-partial (recur ::consume)
                     ::es-down (System/exit 1)
                     ::es-slow (do (Thread/sleep 5000)
                                   (recur ::es-slow)))))
    (msg-chan ([msg]
               (bulk-process-update msg)
               (recur ::consume)))))



(with-fsm state
  (>! msg-chan {:update 1})
  (>! msg-chan {:update 2})
  (>! msg-chan {:update 3})
  (>! msg-chan {:update 4})
  (>! msg-chan {:update 5})

  (is (= state :bulk-success)))
