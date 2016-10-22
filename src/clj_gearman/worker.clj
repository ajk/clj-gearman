(ns clj-gearman.worker
  (:require [clj-gearman.socket :as s]
            [clj-gearman.header :as h]
            [clj-gearman.util :as u]))

(defn send-msg [worker code & args]
  (let [opt (meta worker)]
    (s/write-msg worker h/req (h/code code) (:out-enc opt) args)))

(defn can-do [worker fn-name]
  (send-msg worker "CAN_DO" fn-name))

(defn can-do-timeout [worker fn-name timeout]
  (send-msg worker "CAN_DO_TIMEOUT" fn-name timeout))

(defn cant-do [worker fn-name]
  (send-msg worker "CANT_DO" fn-name))

(defn reset-abilities [worker]
  (send-msg worker "RESET_ABILITIES"))

(defn pre-sleep [worker]
  (send-msg worker "PRE_SLEEP"))

(defn grab-job [worker]
  (s/request worker (h/code "GRAB_JOB")))

(defn grab-job-uniq [worker]
  (s/request worker (h/code "GRAB_JOB_UNIQ")))

(defn grab-job-all [worker]
  (s/request worker (h/code "GRAB_JOB_ALL")))

(defn work-data [worker job-handle data]
  (send-msg worker "WORK_DATA" job-handle data))

(defn work-warning [worker job-handle data]
  (send-msg worker "WORK_WARNING" job-handle data))

(defn work-status [worker job-handle numer denom]
  (send-msg worker "WORK_STATUS" job-handle numer denom))

(defn work-complete [worker job-handle data]
  (send-msg worker "WORK_COMPLETE" job-handle data))

(defn work-fail [worker job-handle]
  (send-msg worker "WORK_FAIL" job-handle))

(defn work-exception [worker job-handle exception]
  (send-msg worker "WORK_EXCEPTION" job-handle exception))

(defn set-client-id [worker uniq]
  (send-msg worker "SET_CLIENT_ID" uniq))


(defn run-task [worker job-handle fn-name workload]
  (let [fun (get-in (meta worker) [:can-do fn-name])]
    (if (= (u/fun-arity fun) 1)
      (try
        (work-complete worker job-handle (fun workload))
        (catch Throwable ex
          (work-exception worker job-handle ex)))
      (fun worker job-handle workload))))


(defn wait-for-task [worker]
  (s/wait-loop
    worker
    (fn []
      (let [[code data] (grab-job worker)]
        (if (= code "JOB_ASSIGN")
          [code data]
          (let [[code data] (s/request worker (h/code "PRE_SLEEP"))]
            (when (= code "NOOP")
              (grab-job worker))))))))

(defn work [worker]
  (let [[code [job-handle fn-name workload]] (wait-for-task worker)]
    (when (= code "JOB_ASSIGN")
      (if (contains? (:can-do (meta worker)) fn-name)
        (run-task worker job-handle fn-name workload)
        (do
          (work-fail worker job-handle)
          (cant-do worker fn-name))))))

(defn connect [w-map]
  (let [worker (s/connect (first (:job-servers w-map)) w-map 10)]
    (doseq [ability (keys (get w-map :can-do {}))]
      (can-do worker (name ability)))
    worker))
