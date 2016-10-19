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
               (c/submit-job client "slow-1" "zero"))

            #(with-open [client (c/connect job-servers)]
               (let [[code job-handle] (c/submit-job-bg client "bg-1" "1")]
                 (Thread/sleep 100)
                 (if (= code "JOB_CREATED")
                  (rest (last (c/get-status client job-handle))))))

            #(with-open [client (c/connect (assoc job-servers :worker-exceptions true))]
               (let [[code [_ ex]] (c/submit-job client "ex-1" "1")]
                 [code ex]))

            ])

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
                              (w/work-complete worker job-handle " and three"))

                  "bg-1" (fn [worker job-handle _]
                           (w/work-status worker job-handle 33 100)
                           (Thread/sleep 300)
                           (w/work-complete worker job-handle ""))

                  "ex-1" (fn [workload]
                           (throw (Exception. "exception in worker")))
                  }))

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
    (is (= ["WORK_COMPLETE" "zero and one and two and three"] (results 1)))
    (is (= '("1" "1" "33" "100") (results 2)))
    (is (= ["WORK_EXCEPTION" "java.lang.Exception: exception in worker"] (results 3)))))


