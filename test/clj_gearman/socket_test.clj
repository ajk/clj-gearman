(ns clj-gearman.socket-test
  (:require [clojure.test :refer :all]
            [clj-gearman.socket :as s]
            [clj-gearman.header :as h]))

; NOTICE: These test will fail unless gearman server is running in port 5555

(defn echo-req [data test-fn]
  (let [sock (s/connect {:host "localhost" :port 5555})]
    (s/write-msg sock h/req (h/code "ECHO_REQ") "UTF-8" [data])
    (test-fn (s/read-msg sock "UTF-8"))
    (s/disconnect sock)))

(deftest raw-echo
  (testing "Send a byte array to gearman server, expect echo response."
    (let [sock (s/connect {:host "localhost" :port 5555})
          out (.getOutputStream @sock)
          in  (.getInputStream @sock)
          bytes-out (byte-array '(0, 82, 69, 81, 0, 0, 0, 16, 0, 0, 0, 4, 101, 99, 104, 111))
          bytes-in  (byte-array '(0, 82, 69, 83, 0, 0, 0, 17, 0, 0, 0, 4, 101, 99, 104, 111))]
      (.write out bytes-out)
      (.flush out)
      (is (= (vec bytes-in) (vec (s/read-bytes in (count bytes-out)))))
      (s/disconnect sock))))

(deftest echo
  (testing "Echo request and response"
    (echo-req "echö"
              (fn [[type code size data]]
                (is (= (vec type) (vec h/res)))
                (is (= code (h/code "ECHO_RES")))
                (is (= 5 size))
                (is (= "echö" (last data)))))))
