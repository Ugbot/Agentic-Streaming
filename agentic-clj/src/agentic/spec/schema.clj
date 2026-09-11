(ns agentic.spec.schema
  "The subset of JSON Schema 2020-12 the v1 workflow and result schemas use, as plain Clojure over the
   wire form (string keys, snake_case). One schema governs YAML, JSON and EDN, so the loader validates
   the wire form the mapping in spec/README.md produces rather than a hand-written mirror of the schema.

   Supported keywords: type, enum, const, required, properties, additionalProperties, patternProperties,
   propertyNames, $ref (#/$defs/...), items, minimum, minItems, minProperties, pattern, anyOf, oneOf,
   default. `apply-defaults` fills documented defaults so a runtime reading an older document sees the
   values the spec promises."
  (:require [clojure.string :as str]))

(defn- resolve-ref [root schema]
  (if-let [ref (get schema "$ref")]
    (let [path (rest (str/split ref #"/"))]
      (resolve-ref root (get-in root path)))
    schema))

(defn- type-of [v]
  (cond (nil? v) "null"
        (boolean? v) "boolean"
        (integer? v) "integer"
        (number? v) "number"
        (string? v) "string"
        (map? v) "object"
        (sequential? v) "array"
        :else "unknown"))

(defn- type-ok? [expected v]
  (let [actual (type-of v)
        wanted (if (string? expected) [expected] expected)]
    (boolean (some (fn [t] (or (= t actual)
                               (and (= t "number") (= actual "integer"))
                               (and (= t "integer") (number? v) (== v (Math/floor (double v))))))
                   wanted))))

(defn- fmt-path [path] (str "/" (str/join "/" (map str path))))

(declare errors)

(defn- property-errors [root schema v path]
  (let [props (get schema "properties" {})
        patterns (get schema "patternProperties" {})
        additional (get schema "additionalProperties" true)
        names-schema (get schema "propertyNames")]
    (concat
     (for [r (get schema "required" []) :when (not (contains? v r))]
       {:path path :message (str "missing required key " r)})
     (when names-schema
       (mapcat (fn [k] (errors root names-schema k (conj path k))) (keys v)))
     (mapcat
      (fn [[k child]]
        (let [kpath (conj path k)
              matched-patterns (filter (fn [[p _]] (re-find (re-pattern p) k)) patterns)]
          (cond
            (contains? props k) (errors root (get props k) child kpath)
            (seq matched-patterns) (mapcat (fn [[_ s]] (errors root s child kpath)) matched-patterns)
            (false? additional) [{:path kpath :message (str "unknown key " k)}]
            (map? additional) (errors root additional child kpath)
            :else nil)))
      v))))

(defn errors
  "Validation problems for `v` against `schema` (both wire form), each {:path [..] :message}."
  ([root v] (errors root root v []))
  ([root schema v path]
   (let [schema (resolve-ref root schema)]
     (cond
       (true? schema) []
       (false? schema) [{:path path :message "no value permitted"}]
       :else
       (->> [(when-let [t (get schema "type")]
               (when-not (type-ok? t v) [{:path path :message (str "expected " t ", got " (type-of v))}]))
             (when (contains? schema "const")
               (when-not (= (get schema "const") v)
                 [{:path path :message (str "expected " (pr-str (get schema "const")) ", got " (pr-str v))}]))
             (when-let [e (get schema "enum")]
               (when-not (some #(= % v) e)
                 [{:path path :message (str (pr-str v) " is not one of " (pr-str e))}]))
             (when-let [m (get schema "minimum")]
               (when (and (number? v) (< v m)) [{:path path :message (str v " is below minimum " m)}]))
             (when-let [p (get schema "pattern")]
               (when (and (string? v) (not (re-find (re-pattern p) v)))
                 [{:path path :message (str (pr-str v) " does not match " p)}]))
             (when (map? v)
               (concat
                (when-let [m (get schema "minProperties")]
                  (when (< (count v) m) [{:path path :message (str "needs at least " m " key(s)")}]))
                (property-errors root schema v path)))
             (when (sequential? v)
               (concat
                (when-let [m (get schema "minItems")]
                  (when (< (count v) m) [{:path path :message (str "needs at least " m " item(s)")}]))
                (when-let [items (get schema "items")]
                  (mapcat (fn [i child] (errors root items child (conj path i))) (range) v))))
             (when-let [branches (get schema "anyOf")]
               (when-not (some #(empty? (errors root % v path)) branches)
                 [{:path path :message "matches none of the permitted alternatives"}]))
             (when-let [branches (get schema "oneOf")]
               (when-not (= 1 (count (filter #(empty? (errors root % v path)) branches)))
                 [{:path path :message "must match exactly one of the permitted alternatives"}]))]
            (apply concat)
            vec)))))

(defn explain
  "A one-line human rendering of a validation problem."
  [{:keys [path message]}]
  (str (fmt-path path) ": " message))

(defn apply-defaults
  "Fill every `default` the schema documents for keys present as objects in `v` (wire form). Defaults
   are applied only inside objects that exist: an absent optional block stays absent."
  ([root v] (apply-defaults root root v))
  ([root schema v]
   (let [schema (resolve-ref root schema)]
     (cond
       (not (map? schema)) v
       (map? v)
       (let [props (get schema "properties" {})
             additional (get schema "additionalProperties")
             with-defaults (reduce-kv (fn [m k s]
                                        (let [s (resolve-ref root s)]
                                          (if (and (not (contains? m k)) (map? s) (contains? s "default"))
                                            (assoc m k (get s "default"))
                                            m)))
                                      v props)]
         (reduce-kv (fn [m k child]
                      (cond
                        (contains? props k) (assoc m k (apply-defaults root (get props k) child))
                        (map? additional) (assoc m k (apply-defaults root additional child))
                        :else m))
                    with-defaults with-defaults))
       (sequential? v)
       (if-let [items (get schema "items")]
         (mapv #(apply-defaults root items %) v)
         v)
       :else v))))

;; ---- the EDN mapping: kebab-case keywords <-> snake_case wire keys ----

(defn- extension-key? [s] (str/starts-with? s "x-"))

(defn key->wire
  "A canonical key to its wire name: structural keywords are snake_cased, `x-` keywords and dynamic
   string keys are verbatim."
  [k]
  (cond
    (string? k) k
    (keyword? k) (let [n (name k)] (if (extension-key? n) n (str/replace n "-" "_")))
    :else (str k)))

(defn- wire->keyword [s]
  (keyword (if (extension-key? s) s (str/replace s "_" "-"))))

(defn- key-name [k] (if (keyword? k) (name k) (str k)))

(defn- scalar-string-schema? [schema]
  (or (contains? schema "enum") (contains? schema "const")
      (let [t (get schema "type")] (or (= t "string") (and (sequential? t) (some #{"string"} t))))))

(defn canonicalize
  "Read any of the three syntaxes into the canonical Clojure form, guided by the schema: keys the schema
   declares become kebab-case keywords, `x-` extension keys stay verbatim keywords, and keys the schema
   leaves open (path names, router rules, tool triggers, runtime blocks) stay strings so their spelling
   survives the round trip. Input keys may be strings or keywords in either casing. Keyword scalars in
   EDN (`:rule` for `rule`) are read as strings where the schema asks for a string."
  ([root v] (canonicalize root root v))
  ([root schema v]
   (let [schema (resolve-ref root schema)]
     (cond
       (not (map? schema)) v
       (map? v)
       (let [props (get schema "properties" {})
             patterns (keys (get schema "patternProperties" {}))
             additional (get schema "additionalProperties" true)]
         (reduce-kv
          (fn [m k child]
            (let [raw (key-name k)
                  wire (if (extension-key? raw) raw (str/replace raw "-" "_"))]
              (cond
                (contains? props wire) (assoc m (wire->keyword wire) (canonicalize root (get props wire) child))
                (some #(re-find (re-pattern %) raw) patterns) (assoc m (keyword raw) child)
                (map? additional) (assoc m raw (canonicalize root additional child))
                :else (assoc m raw child))))
          {} v))
       (sequential? v)
       (if-let [items (get schema "items")]
         (mapv #(canonicalize root items %) v)
         (vec v))
       (and (keyword? v) (scalar-string-schema? schema)) (name v)
       :else v))))

(defn ->wire
  "The canonical form back to the wire form (string keys, snake_case) — what validation and the other
   runtimes see. Lossless with `canonicalize`."
  [v]
  (cond
    (map? v) (reduce-kv (fn [m k child] (assoc m (key->wire k) (->wire child))) {} v)
    (sequential? v) (mapv ->wire v)
    (keyword? v) (name v)
    :else v))
