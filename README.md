# clj-gearman

This library provides Clojure bindings for [gearman](http://gearman.org/) distributed job system.

[You can find the protocol specification here.](http://gearman.org/protocol/)

This implementation uses regular blocking sockets.
Take a look at [clearman](https://github.com/joshrotenberg/clearman) if you need async IO.

# Installation

Add the following dependency to your project.clj file:

[![Clojars Project](https://clojars.org/clj-gearman/latest-version.svg)](https://clojars.org/clj-gearman)

# Synopsis

Worker side:

```clojure
(ns my-worker
  (:require [clojure.string :as str]
            [clj-gearman.worker :as w]))

(def worker

   ; List of job servers you have running.
   ; Currently only the first one is used.
  {:job-servers [{:host "localhost" :port 4730}]

   ; String encoding for input and output.
   ; You probably want to keep these utf-8.
   :in-enc  "UTF-8"
   :out-enc "UTF-8"

   ; Map of your worker capability functions.
   ; Notice that keys should be strings.
   :can-do {
            ; First is the ever popular string reversal.
            ; Workload parameter is always a string and your return value must
            ; also be a single string.
            "reverse" (fn [workload] (str/reverse workload))

            ; If you like a more hands-on approach you can operate on the worker socket directly.
            ; In this case you are responsible for all communication with the job server.
            "long-running" (fn [socket job-handle workload]
                              (try
                               (do
                                 (your-slow-operation-one workload)
                                 (w/work-status socket job-handle 1 3)
                                 (your-slow-operation-two workload)
                                 (w/work-status socket job-handle 2 3)
                                 (your-slow-operation-three workload)
                                 (w/work-status socket job-handle 3 3)
                               (catch Throwable ex
                                 (w/work-exception socket job-handle ex))))}})

; Connect to job server and start accepting tasks.
(with-open [socket (w/connect worker)]
  (while true (w/work socket)))

```


Client side:

```clojure
(ns my-client
  (:require [clojure.pprint :refer [pprint]]
            [clj-gearman.client :as c]))

(def client

   ; List of job servers you have running.
   ; Currently only the first one is used.
  {:job-servers [{:host "localhost" :port 4730}]

   ; String encoding for input and output.
   ; You probably want to keep these utf-8.
   :in-enc  "UTF-8"
   :out-enc "UTF-8"

   ; If true, exceptions thrown in worker will be propagated through
   ; the job server to this client.
   :worker-exceptions false})

; Connect to job server and submit the work requests.
(with-open [socket (c/connect client)]
  (let [[code result :as response] (c/submit-job socket "reverse" "foo bar baz")]
    (if (= code "WORK_COMPLETE")
      (println result)
      (throw (Exception. (pr-str response)))))

  ; Fire-and-forget background job.
  ; Use this if you don't need the final return value from the worker.
  (let [[code job-handle :as response] (c/submit-job-bg socket "long-running" "Our workload string")]
    (if (= code "JOB_CREATED")
      ; Optionally poll for status of the created task.
      (dotimes [_ 10]
        (Thread/sleep 1000)
        (pprint (c/get-status socket job-handle)))
      (throw (Exception. (pr-str response))))))

```

# API documentation

[Read the full api docs here.](https://ajk.github.io/clj-gearman/index.html)


# Contributing

Feel free to send bug reports and pull requests on this repository.

# License

Copyright 2016 Antti Koskinen

Distributed under the Eclipse Public License, the same as Clojure.
