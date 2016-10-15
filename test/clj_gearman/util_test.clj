(ns clj-gearman.util-test
  (:require [clojure.test :refer :all]
            [clj-gearman.util :as u]))

(defn flatten-helper [msg]
  (->> msg
       (map vec)
       (apply concat)
       byte-array))

(deftest convert-ints
  (testing "Convert integers to byte arrays and back."
    (is (= 0 (u/bytea->int (u/int->bytea 0))))
    (is (= 1 (u/bytea->int (u/int->bytea 1))))
    (is (= -123 (u/bytea->int (u/int->bytea -123))))
    (is (= 1024 (u/bytea->int (u/int->bytea 1024))))
    (is (= 2147483647 (u/bytea->int (u/int->bytea 2147483647))))
    (is (thrown? java.lang.IllegalArgumentException (u/int->bytea 2147483648)))))

(deftest convert-strings
  (testing "Convert strings to byte arrays and back."
    (is (= "" (u/bytea->str (u/str->bytea ""))))
    (is (= "abc..åäö" (u/bytea->str (u/str->bytea "abc..åäö"))))
    (is (= "abc" (u/bytea->str (u/str->bytea "abc" "ASCII") "ASCII")))
    (is (not= "åäö" (u/bytea->str (u/str->bytea "åäö") "ASCII")))
    (is (= "åäö" (u/bytea->str (u/str->bytea "åäö" nil) "UTF-8")))
    (is (= "åäö" (u/bytea->str (u/str->bytea "åäö" "UTF-16") "UTF-16")))
    (is (thrown? java.io.UnsupportedEncodingException (u/str->bytea "foo" "FOO")))))

; For testing purposes, flatten our null-separated byte array

(deftest convert-msg
  (testing "Convert message payload to null-separated byte arrays and back"
    (is (= []                  (-> []                  (u/msg->bytea "ASCII") flatten-helper (u/bytea->msg "ASCII"))))
    (is (= ["echo"]            (-> ["echo"]            (u/msg->bytea "ASCII") flatten-helper (u/bytea->msg "ASCII"))))
    (is (= ["Foo" "Bar" "Bäz"] (-> ["Foo" "Bar" "Bäz"] (u/msg->bytea "UTF-8") flatten-helper (u/bytea->msg "UTF-8"))))))
