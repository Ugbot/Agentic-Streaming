(ns agentic.fixtures
  "Repository-root fixture resolution for the parity tests. The shared examples/pipelines/*.yaml
   files are located by walking up from the working directory until a directory holds both
   reactor/pom.xml and examples/pipelines. A missing fixture throws; tests never skip because of
   the directory the runner was started from."
  (:require [clojure.java.io :as io])
  (:import [java.io File]))

(defn- repo-root? [^File dir]
  (and (.isFile (io/file dir "reactor" "pom.xml"))
       (.isDirectory (io/file dir "examples" "pipelines"))))

(defn repo-root
  "The nearest ancestor of user.dir that is the repository root."
  ^File []
  (loop [dir (.getAbsoluteFile (io/file (System/getProperty "user.dir")))]
    (cond
      (nil? dir) (throw (ex-info "repository root (reactor/pom.xml and examples/pipelines) not found"
                                 {:user-dir (System/getProperty "user.dir")}))
      (repo-root? dir) dir
      :else (recur (.getParentFile dir)))))

(defn example-pipeline
  "Absolute path of <repo>/examples/pipelines/<name>; throws when the file does not exist."
  ^String [name]
  (let [f (io/file (repo-root) "examples" "pipelines" name)]
    (when-not (.isFile f)
      (throw (ex-info (str "required fixture is missing: " (.getPath f)) {:path (.getPath f)})))
    (.getPath f)))
