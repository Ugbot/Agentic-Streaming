(ns agentic.build.version
  "Derives the agentic-clj artifact version from the repository's release tags.

  The scheme is the one setuptools-scm applies to the four Python distributions (see
  tools/check_release_version.py at the repository root), so one tag gives one version string
  everywhere:

    tag v1.0.0rc1 checked out exactly, clean tree   -> 1.0.0rc1
    tag v1.0.0rc1 plus 3 commits (or a dirty tree)  -> 1.0.0rc2.dev3   (guess the next version, .devN)
    tag v1.0.0 plus 3 commits                       -> 1.0.1.dev3
    no v* tag reachable, 199 commits                -> 0.1.dev199

  A tag is `v` followed by a canonical PEP 440 public version; anything else (1.0, v1.0.0-rc1,
  v01.0.0, v1.0.0+local) is rejected instead of being coerced. Only pure functions live here apart
  from `from-git`, which shells out to git; the test suite exercises the pure part."
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]))

(def ^:private canonical
  #"^v?(?:[1-9][0-9]*!)?(?:0|[1-9][0-9]*)(?:\.(?:0|[1-9][0-9]*))*(?:(?:a|b|rc)(?:0|[1-9][0-9]*))?(?:\.post(?:0|[1-9][0-9]*))?(?:\.dev(?:0|[1-9][0-9]*))?$")

(defn tag->version
  "The version a release tag names, or throws when the tag is not `v` + a canonical PEP 440 version."
  [tag]
  (let [tag (str/replace (str/trim (str tag)) #"^refs/tags/" "")]
    (when-not (re-matches canonical tag)
      (throw (ex-info (str "release tag '" tag "' is not v<canonical PEP 440 version>"
                           " (for example v1.0.0, v1.0.0a1, v1.0.0rc1)")
                      {:tag tag})))
    (str/replace tag #"^v" "")))

(defn guess-next
  "setuptools-scm's guess-next-dev rule: bump the trailing number of the version
  (1.0.0 -> 1.0.1, 1.0.0rc1 -> 1.0.0rc2, 1.0.0.post2 -> 1.0.0.post3)."
  [version]
  (let [[_ head n] (re-matches #"^(.*?)(\d+)$" version)]
    (str head (inc (Long/parseLong n)))))

(defn derive-version
  "The version for a working tree described by `tag` (the newest reachable v* tag, or nil),
  `distance` (commits since that tag, or since the root when there is no tag) and `dirty?`."
  [{:keys [tag distance dirty?]}]
  (let [distance (long (or distance 0))]
    (cond
      (nil? tag) (str "0.1.dev" distance)
      (and (zero? distance) (not dirty?)) (tag->version tag)
      :else (str (guess-next (tag->version tag)) ".dev" distance))))

(defn- git [dir & args]
  (let [{:keys [exit out err]} (apply sh/sh "git" "-C" (str dir) args)]
    (when-not (zero? exit)
      (throw (ex-info (str "git " (str/join " " args) " failed: " (str/trim (str err))) {:exit exit})))
    (str/trim out)))

(defn describe
  "Reads {:tag :distance :dirty? :sha} for the repository containing `dir` with git."
  [dir]
  (let [tag (let [{:keys [exit out]} (sh/sh "git" "-C" (str dir) "describe" "--tags" "--abbrev=0" "--match" "v*")]
              (when (zero? exit) (str/trim out)))
        distance (Long/parseLong (git dir "rev-list" "--count" (if tag (str tag "..HEAD") "HEAD")))
        dirty? (not (str/blank? (git dir "status" "--porcelain" "--untracked-files=no")))]
    {:tag tag
     :distance distance
     :dirty? dirty?
     :sha (git dir "rev-parse" "--short" "HEAD")}))

(defn from-git
  "The artifact version of the checkout containing `dir`."
  [dir]
  (derive-version (describe dir)))
