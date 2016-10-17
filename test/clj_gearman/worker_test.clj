(ns clj-gearman.worker-test
  (:require [clojure.test :refer :all]
            [clojure.pprint :refer [pprint]]
            [clj-gearman.worker :as w]
            [clj-gearman.client :as c]))

; NOTICE: These tests will fail unless gearman server is running in port 5555
;
; $ gearmand --verbose DEBUG -p5555


(def job-servers {:job-servers [{:host "localhost" :port 5555}]})

(def tasks [#(with-open [client (c/connect job-servers)]
               (c/submit-job client "reverse" "foo bar baz"))
            #(with-open [client (c/connect job-servers)]
               (c/submit-job client "slow-1" "zero"))])

(def my-worker
  (assoc job-servers
         :can-do {"reverse" (fn [s] (apply str (reverse s)))
                  "slow-1"  (fn [worker job-handle workload]
                              (w/work-data worker job-handle workload)
                              (Thread/sleep 100)
                              (w/work-data worker job-handle " and one")
                              (Thread/sleep 100)
                              (w/work-data worker job-handle " and two")
                              (Thread/sleep 100)
                              (w/work-complete worker job-handle " and three"))}))

(defn run-worker []
  (with-open [worker (w/connect my-worker)]
    (dotimes [_ (count tasks)]
      (w/work worker))))


(def t-1 (doto (Thread. #(run-worker)) (.start)))

; wait a bit for worker connection
(Thread/sleep 500)

(def results (mapv #(%) tasks))

(.join t-1)


(deftest worker-responses
  (testing "Responses to client SUBMIT_JOB requests"
    (is (= ["WORK_COMPLETE" "zab rab oof"] (results 0)))
    (is (= ["WORK_COMPLETE" "zero and one and two and three"] (results 1)))))


