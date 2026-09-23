(ns build
  "tools.build tasks for the agentic-clj library artifact.

    clojure -T:build version   ; print the version derived from the release tags
    clojure -T:build clean     ; remove target/
    clojure -T:build jar       ; target/agentic-clj-<version>.jar with its pom (and target/pom.xml)
    clojure -T:build install   ; the same jar and pom into the local ~/.m2 repository

  Coordinates: io.github.ugbot/agentic-clj. The version comes from the same tag scheme as the
  Python distributions (agentic.build.version); nothing here talks to Clojars or any other remote
  repository, see docs/release.md at the repository root for the publishing steps."
  (:require [agentic.build.version :as version]
            [clojure.java.io :as io]
            [clojure.tools.build.api :as b]))

(def lib 'io.github.ugbot/agentic-clj)
(def scm-url "https://github.com/Ugbot/Agentic-Streaming")

(def target "target")
(def class-dir (str target "/classes"))
(def src-dirs ["src"])
(def resource-dirs (filterv #(.isDirectory (io/file %)) ["resources"]))

(defn- version* [] (version/from-git "."))
(defn- jar-file [v] (format "%s/%s-%s.jar" target (name lib) v))

(defn- scm-tag
  "The <scm><tag> for the pom: the release tag when the build is exactly at one, else the commit."
  [v]
  (let [{:keys [tag distance dirty? sha]} (version/describe ".")]
    (if (and tag (zero? distance) (not dirty?) (= v (version/tag->version tag))) tag sha)))

(defn version
  "Prints the artifact version that jar and install use."
  [_]
  (println (version*)))

(defn clean
  "Deletes target/."
  [_]
  (b/delete {:path target}))

(defn jar
  "Writes the pom and builds the jar. Returns {:version :jar-file :pom-file}."
  [_]
  (let [v (version*)
        basis (b/create-basis {:project "deps.edn"})
        jar-path (jar-file v)
        tag (scm-tag v)]
    (clean nil)
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version v
                  :basis basis
                  :src-dirs src-dirs
                  :resource-dirs resource-dirs
                  :scm {:url scm-url
                        :connection (str "scm:git:" scm-url ".git")
                        :developerConnection (str "scm:git:" scm-url ".git")
                        :tag tag}
                  :pom-data [[:description "Agentic Clojure: the agentic/v1 runtime on Datomic"]
                             [:url scm-url]
                             [:licenses
                              [:license
                               [:name "Apache License, Version 2.0"]
                               [:url "https://www.apache.org/licenses/LICENSE-2.0"]]]]})
    (b/copy-dir {:src-dirs (into src-dirs resource-dirs) :target-dir class-dir})
    (b/copy-file {:src "../LICENSE" :target (str class-dir "/META-INF/LICENSE")})
    (b/jar {:class-dir class-dir :jar-file jar-path})
    (let [pom (b/pom-path {:class-dir class-dir :lib lib})
          pom-copy (str target "/pom.xml")]
      (b/copy-file {:src pom :target pom-copy})
      (println (str lib " " v))
      (println jar-path)
      (println pom-copy)
      {:version v :jar-file jar-path :pom-file pom-copy})))

(defn install
  "Builds the jar and installs it with its pom into the local Maven repository (~/.m2)."
  [_]
  (let [{:keys [version jar-file]} (jar nil)
        basis (b/create-basis {:project "deps.edn"})]
    (b/install {:basis basis
                :lib lib
                :version version
                :jar-file jar-file
                :class-dir class-dir})
    (println (str "installed " lib " " version " into "
                  (System/getProperty "user.home") "/.m2/repository/io/github/ugbot/agentic-clj/" version))))
