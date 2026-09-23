(ns agentic.build-version-test
  "agentic.build.version lives under build/ (the :build alias path, not the library classpath),
  so it is loaded from its file here; the assertions pin the artifact version to the tag scheme
  setuptools-scm applies to the Python distributions."
  (:require [clojure.test :refer [deftest is testing]]))

(load-file "build/agentic/build/version.clj")
(alias 'v 'agentic.build.version)

(deftest exact-tags-become-versions
  (doseq [[tag version] {"v1.0.0" "1.0.0"
                         "1.0.0" "1.0.0"
                         "refs/tags/v1.0.0a1" "1.0.0a1"
                         "v1.0.0b2" "1.0.0b2"
                         "v1.0.0rc1" "1.0.0rc1"
                         "v2.1.3.post1" "2.1.3.post1"
                         "v0.1.dev199" "0.1.dev199"
                         "v1!2.0" "1!2.0"}]
    (is (= version (v/tag->version tag)) tag)))

(deftest noncanonical-tags-are-rejected
  (doseq [tag ["1.0.0-rc1" "01.0.0" "1.0.0RC1" "1.0.0+local" "release-1" "v1.0.0." "1.0.0rc"
               "v1.0.0.a1" "" "v"]]
    (is (thrown? clojure.lang.ExceptionInfo (v/tag->version tag)) tag)))

(deftest next-version-bumps-the-trailing-number
  (is (= "1.0.1" (v/guess-next "1.0.0")))
  (is (= "1.0.0rc2" (v/guess-next "1.0.0rc1")))
  (is (= "1.0.0a10" (v/guess-next "1.0.0a9")))
  (is (= "1.0.0.post3" (v/guess-next "1.0.0.post2"))))

(deftest derived-versions-follow-setuptools-scm
  (testing "exactly at a clean tag: the tag's version"
    (is (= "1.0.0rc1" (v/derive-version {:tag "v1.0.0rc1" :distance 0 :dirty? false}))))
  (testing "commits after a tag: next version, .dev<distance>"
    (is (= "1.0.1.dev3" (v/derive-version {:tag "v1.0.0" :distance 3 :dirty? false})))
    (is (= "1.0.0rc2.dev3" (v/derive-version {:tag "v1.0.0rc1" :distance 3 :dirty? false}))))
  (testing "dirty tree at a tag: next version, .dev0"
    (is (= "1.0.1.dev0" (v/derive-version {:tag "v1.0.0" :distance 0 :dirty? true}))))
  (testing "no tag reachable: 0.1.dev<commits>"
    (is (= "0.1.dev199" (v/derive-version {:tag nil :distance 199 :dirty? false})))
    (is (= "0.1.dev0" (v/derive-version {:tag nil :distance 0 :dirty? true})))))

(deftest the-checkout-describes-itself
  (let [{:keys [distance dirty? sha]} (v/describe ".")]
    (is (nat-int? distance))
    (is (boolean? dirty?))
    (is (re-matches #"[0-9a-f]{7,}" sha))
    (is (string? (v/from-git ".")))))
