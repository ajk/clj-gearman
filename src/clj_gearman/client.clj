(ns clj-gearman.client
  (:require [clj-gearman.socket :as s]
            [clj-gearman.header :as h]
            [clj-gearman.util   :as u]))


(defn read-response [socket]
  (let [opt (meta socket)
        error? #{"WORK_FAIL" "WORK_EXCEPTION" "ERROR"}
        done?  #{"WORK_COMPLETE"}
        chunk? #{"WORK_DATA"}
        warn?  #{"WORK_WARNING"}]

    (loop [result ""]
      (let [[code data] (s/response socket opt)]
        (when (warn? code) (.print *err* (last data)))
        (cond
          (error? code) [code data]
          (done?  code) [code (str result (last data))]
          :else
          (recur (if (chunk? code) (str result (last data)) result)))))))

(defn submit-job
  ([socket fn-name data] (submit-job socket fn-name data {}))

  ([socket fn-name data opt]
  (let [res (s/request socket
                     (:code opt (h/code "SUBMIT_JOB"))
                     fn-name
                     (:uniq opt (u/new-uniq))
                     data)]
    (if (= (res 0) "JOB_CREATED")
      (read-response socket)
      res))))


(defn submit-job-high
  ([socket fn-name data] (submit-job-high socket fn-name data {}))
  ([socket fn-name data opt]
   (submit-job socket fn-name data (assoc opt :code (h/code "SUBMIT_JOB_HIGH")))))


(defn submit-job-low
  ([socket fn-name data] (submit-job-low socket fn-name data {}))
  ([socket fn-name data opt]
   (submit-job socket fn-name data (assoc opt :code (h/code "SUBMIT_JOB_LOW")))))


(defn submit-job-bg
  ([socket fn-name data] (submit-job-bg socket fn-name data {}))
  ([socket fn-name data opt]
   (let [[code [job-handle] :as response] (s/request socket
                                                   (:code opt (h/code "SUBMIT_JOB_BG"))
                                                   fn-name
                                                   (:uniq opt (u/new-uniq))
                                                   data)]
     (if (= code "JOB_CREATED")
       [code job-handle]
       response))))

(defn submit-job-high-bg
  ([socket fn-name data] (submit-job-high-bg socket fn-name data {}))
  ([socket fn-name data opt]
   (submit-job-bg socket fn-name data (assoc opt :code (h/code "SUBMIT_JOB_HIGH_BG")))))

(defn submit-job-low-bg
  ([socket fn-name data] (submit-job-high-bg socket fn-name data {}))
  ([socket fn-name data opt]
   (submit-job-bg socket fn-name data (assoc opt :code (h/code "SUBMIT_JOB_LOW_BG")))))


(defn get-status [socket job-handle]
  (s/request socket (h/code "GET_STATUS") job-handle))

(defn get-status-unique [socket uniq]
  (s/request socket (h/code "GET_STATUS_UNIQUE") uniq))

(defn option-req [socket opt]
  (s/request socket (h/code "OPTION_REQ") opt))

(defn echo
  [socket data]
  (let [[code [response]] (s/request socket (h/code "ECHO_REQ") data)]
    [code response]))

(defn connect [client]
  (let [socket (s/connect (first (:job-servers client)) client 10)]
    (when (:worker-exceptions client)
      (option-req socket "exceptions"))
    socket))

