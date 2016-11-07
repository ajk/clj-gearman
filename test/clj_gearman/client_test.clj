(ns clj-gearman.client-test
  (:require [clojure.test :refer :all]
            [clj-gearman.client :as c]))

; NOTICE: These test will fail unless gearman server is running in port 5555

; First job server doesn't exist but connection to second one should succeed.
(def my-client {:job-servers [{:host "localhost" :port 5554}
                              {:host "localhost" :port 5555}]})

(deftest echo
  (testing "Echo request and response"
    (with-open [client (c/connect my-client)]
      (is (= ["ECHO_RES" "echö"] (c/echo client "echö"))))))
