(ns clj-gearman.client
  (:require [clj-gearman.socket :as s]
            [clj-gearman.header :as h]
            [clj-gearman.util   :as u]))


(defn read-response [client]
  (let [opt (meta client)
        error? #{"WORK_FAIL" "WORK_EXCEPTION" "ERROR"}
        done?  #{"WORK_COMPLETE"}
        chunk? #{"WORK_DATA"}
        warn?  #{"WORK_WARNING"}]

    (loop [result ""]
      (let [[code data] (s/response client opt)]
        (when (warn? code) (.print *err* (last data)))
        (cond
          (error? code) [code data]
          (done?  code) [code (str result (last data))]
          :else
          (recur (if (chunk? code) (str result (last data)) result)))))))

(defn submit-job
  ([client fn-name data] (submit-job client fn-name data {}))

  ([client fn-name data opt]
  (let [res (s/request client
                     (:code opt (h/code "SUBMIT_JOB"))
                     fn-name
                     (:uniq opt (u/new-uniq))
                     data)]
    (if (= (res 0) "JOB_CREATED")
      (read-response client)
      res))))


(defn submit-job-high
  ([client fn-name data] (submit-job-high client fn-name data {}))
  ([client fn-name data opt]
   (submit-job client fn-name data (assoc opt :code (h/code "SUBMIT_JOB_HIGH")))))


(defn submit-job-low
  ([client fn-name data] (submit-job-low client fn-name data {}))
  ([client fn-name data opt]
   (submit-job client fn-name data (assoc opt :code (h/code "SUBMIT_JOB_LOW")))))


(defn submit-job-bg
  ([client fn-name data] (submit-job-bg client fn-name data {}))
  ([client fn-name data opt]
   (s/request client
            (:code opt (h/code "SUBMIT_JOB_BG"))
            fn-name
            (:uniq opt (u/new-uniq))
            data)))

(defn submit-job-high-bg
  ([client fn-name data] (submit-job-high-bg client fn-name data {}))
  ([client fn-name data opt]
   (submit-job-bg client fn-name data (assoc opt :code (h/code "SUBMIT_JOB_HIGH_BG")))))

(defn submit-job-low-bg
  ([client fn-name data] (submit-job-high-bg client fn-name data {}))
  ([client fn-name data opt]
   (submit-job-bg client fn-name data (assoc opt :code (h/code "SUBMIT_JOB_LOW_BG")))))


(defn get-status [client job-handle]
  (s/request client (h/code "GET_STATUS") job-handle))

(defn get-status-unique [client uniq]
  (s/request client (h/code "GET_STATUS_UNIQUE") uniq))

(defn echo
  [client data]
  (let [[code [response]] (s/request client (h/code "ECHO_REQ") data)]
    [code response]))

(defn connect [c-map]
  (s/connect (first (:job-servers c-map)) c-map))

