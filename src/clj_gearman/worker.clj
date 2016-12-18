(ns clj-gearman.worker
  "Gearman worker functions."
  (:require [clj-gearman.socket :as s]
            [clj-gearman.header :as h]
            [clj-gearman.pool :as p]
            [clj-gearman.util :as u]))

(defn send-msg [socket code & args]
  (let [opt (meta socket)]
    (s/write-msg socket h/req (h/code code) (:out-enc opt) args)))

(defn can-do [socket fn-name]
  (send-msg socket "CAN_DO" fn-name))

(defn can-do-timeout [socket fn-name timeout]
  (send-msg socket "CAN_DO_TIMEOUT" fn-name timeout))

(defn cant-do [socket fn-name]
  (send-msg socket "CANT_DO" fn-name))

(defn reset-abilities [socket]
  (send-msg socket "RESET_ABILITIES"))

(defn pre-sleep [socket]
  (send-msg socket "PRE_SLEEP"))

(defn grab-job [socket]
  (s/request socket (h/code "GRAB_JOB")))

(defn grab-job-uniq [socket]
  (s/request socket (h/code "GRAB_JOB_UNIQ")))

(defn grab-job-all [socket]
  (s/request socket (h/code "GRAB_JOB_ALL")))

(defn work-data [socket job-handle data]
  (send-msg socket "WORK_DATA" job-handle data))

(defn work-warning [socket job-handle data]
  (send-msg socket "WORK_WARNING" job-handle data))

(defn work-status [socket job-handle numer denom]
  (send-msg socket "WORK_STATUS" job-handle numer denom))

(defn work-complete [socket job-handle data]
  (send-msg socket "WORK_COMPLETE" job-handle data))

(defn work-fail [socket job-handle]
  (send-msg socket "WORK_FAIL" job-handle))

(defn work-exception [socket job-handle exception]
  (send-msg socket "WORK_EXCEPTION" job-handle exception))

(defn set-client-id [socket uniq]
  (send-msg socket "SET_CLIENT_ID" uniq))


(defn run-task [socket job-handle fn-name workload]
  (let [fun (get-in (meta socket) [:can-do fn-name])]
    (if (= (u/fun-arity fun) 1)
      (try
        (work-complete socket job-handle (fun workload))
        (catch Throwable ex
          (work-exception socket job-handle ex)))
      (fun socket job-handle workload))))


(defn wait-for-task [socket]
  (s/with-timeout
    socket
    (fn []
      (let [[code data] (grab-job socket)]
        (if (= code "JOB_ASSIGN")
          [code data]
          (let [[code data] (s/request socket (h/code "PRE_SLEEP"))]
            (when (= code "NOOP")
              (grab-job socket))))))))

(defn work [socket]
  (let [[code [job-handle fn-name workload]] (wait-for-task socket)]
    (when (= code "JOB_ASSIGN")
      (if (contains? (:can-do (meta socket)) fn-name)
        (run-task socket job-handle fn-name workload)
        (do
          (work-fail socket job-handle)
          (cant-do socket fn-name))))))

(defn connect [worker]
  (let [socket (s/connect (first (:job-servers worker)) worker 10)]
    (doseq [ability (keys (get worker :can-do {}))]
      (can-do socket (name ability)))
    socket))

(defn pool
  "Create a pool of worker threads and start accepting tasks.
  Returns a function which will stop the pool when called."
  [worker]
  (p/worker-pool worker connect work))
